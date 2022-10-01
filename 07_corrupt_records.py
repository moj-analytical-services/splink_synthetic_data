import os

import logging


import pandas as pd
import numpy as np

import duckdb

from corrupt.corrupt_string import string_corrupt_numpad

from corrupt.corruption_functions import (
    master_record_no_op,
    basic_null_fn,
    format_master_data,
    generate_uncorrupted_output_record,
    format_master_record_first_array_item,
)


from corrupt.corrupt_occupation import (
    occupation_format_master_record,
    occupation_gen_uncorrupted_record,
    occupation_corrupt,
)

from corrupt.corrupt_name import (
    full_name_gen_uncorrupted_record,
    full_name_alternative,
    each_name_alternatives,
    full_name_typo,
    full_name_null,
)


from corrupt.corrupt_date import (
    date_corrupt_timedelta,
    date_gen_uncorrupted_record,
)

from corrupt.corrupt_country_citizenship import (
    country_citizenship_format_master_record,
    country_citizenship_corrupt,
    country_citizenship_gen_uncorrupted_record,
)

from path_fns.filepaths import TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON

from corrupt.corrupt_lat_lng import lat_lng_uncorrupted_record, lat_lng_corrupt_distance

from functools import partial
from corrupt.geco_corrupt import get_zipf_dist


logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(message)s",
)
logger.setLevel(logging.INFO)


con = duckdb.connect()

in_path = os.path.join(
    TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON, "transformed_master_data.parquet"
)


sql = f"""
select *
from '{in_path}'
limit 5
"""

pd.options.display.max_columns = 1000
raw_data = con.execute(sql).df()
raw_data

# Configure how corruptions will be made for each field

# Col name is the OUTPUT column name.  For instance, we may input given name,
# family name etc to output full_name

# Guide to keys:
# format_master_data.  This functino may apply additional cleaning to the master
# record.  The same formatted master ata is then available to the
# 'gen_uncorrupted_record' and 'corruption_functions'


config = [
    {
        "col_name": "full_name",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": full_name_gen_uncorrupted_record,
    },
    {
        "col_name": "occupation",
        "format_master_data": occupation_format_master_record,
        "gen_uncorrupted_record": occupation_gen_uncorrupted_record,
    },
    {
        "col_name": "dob",
        "format_master_data": partial(
            format_master_record_first_array_item, colname="dob"
        ),
        "gen_uncorrupted_record": partial(
            date_gen_uncorrupted_record, input_colname="dob", output_colname="dob"
        ),
    },
    {
        "col_name": "birth_coordinates",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": partial(
            lat_lng_uncorrupted_record,
            input_colname="birth_coordinates",
            output_colname="birth_coordinates",
        ),
    },
    {
        "col_name": "residence_coordinates",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": partial(
            lat_lng_uncorrupted_record,
            input_colname="residence_coordinates",
            output_colname="residence_coordinates",
        ),
    },
    {
        "col_name": "country_citizenLabel",
        "format_master_data": country_citizenship_format_master_record,
        "gen_uncorrupted_record": country_citizenship_gen_uncorrupted_record,
    },
]

from corrupt.error_vector import (
    CompositeCorruption,
    RecordCorruptor,
    ProbabilityAdjustmentFromLookup,
    ProbabilityAdjustmentFromSQL,
    name_inversion,
    initial,
)

rc = RecordCorruptor()

name_inversion_corruption = CompositeCorruption(
    name="name_inversion_corruption", baseline_probability=0.9
)
name_inversion_corruption.add_corruption_function(
    name_inversion, args={"col1": "first_name", "col2": "surname"}
)
rc.add_composite_corruption(name_inversion_corruption)


initital_corruption = CompositeCorruption(
    "first_name_initial_corruption", baseline_probability=0.9
)
initital_corruption.add_corruption_function(initial, args={"col": "first_name"})
rc.add_composite_corruption(initital_corruption)

# Add adjustments
corruption_lookup = {
    "ethnicity": {
        "white": [(name_inversion_corruption, 10.0)],
        "asian": [(name_inversion_corruption, 2.0)],
    },
    "full_name": {"robin": [(initital_corruption, 10.0)]},
}

adjustment = ProbabilityAdjustmentFromLookup(corruption_lookup)
rc.add_probability_adjustment(adjustment)

sql_condition = "len(first_name) > 3 and len(surname) > 3"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, initital_corruption, 0.001)
rc.add_probability_adjustment(adjustment)

max_corrupted_records = 3
zipf_dist = get_zipf_dist(max_corrupted_records)

records = raw_data.to_dict(orient="records")


output_records = []
for i, master_input_record in enumerate(records):

    # Formats the input data into an easy format for producing
    # an uncorrupted/corrupted outputs records
    formatted_master_record = format_master_data(master_input_record, config)

    uncorrupted_output_record = generate_uncorrupted_output_record(
        formatted_master_record, config
    )
    uncorrupted_output_record["ethnicity"] = "white"

    output_records.append(uncorrupted_output_record)

    # How many corrupted records to generate
    total_num_corrupted_records = np.random.choice(
        zipf_dist["vals"], p=zipf_dist["weights"]
    )

    record = uncorrupted_output_record.copy()
    rc.apply_probability_adjustments(record)
    corrupted_record = rc.apply_corruptions_to_record(record)
    output_records.append(corrupted_record)

df = pd.DataFrame(output_records)

df.head(20)
