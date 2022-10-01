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
    date_corrupt_jan_first,
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


from corrupt.record_corruptor import (
    CompositeCorruption,
    RecordCorruptor,
    ProbabilityAdjustmentFromSQL,
)


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
display(raw_data)

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
    # {
    #     "col_name": "occupation",
    #     "format_master_data": occupation_format_master_record,
    #     "gen_uncorrupted_record": occupation_gen_uncorrupted_record,
    # },
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
        "col_name": "dod",
        "format_master_data": partial(
            format_master_record_first_array_item, colname="dod"
        ),
        "gen_uncorrupted_record": partial(
            date_gen_uncorrupted_record, input_colname="dod", output_colname="dod"
        ),
    },
    # {
    #     "col_name": "birth_coordinates",
    #     "format_master_data": master_record_no_op,
    #     "gen_uncorrupted_record": partial(
    #         lat_lng_uncorrupted_record,
    #         input_colname="birth_coordinates",
    #         output_colname="birth_coordinates",
    #     ),
    # },
    # {
    #     "col_name": "country_citizenLabel",
    #     "format_master_data": country_citizenship_format_master_record,
    #     "gen_uncorrupted_record": country_citizenship_gen_uncorrupted_record,
    # },
]


rc = RecordCorruptor()


########
# Date of birth and date of death corruptions
########

# Create a timedelta corruption with baseline probability 20%
# This is a simple independent corruption function that's not affected
# by the presence or absence of other corruptions, or the values in the data
rc.add_simple_corruption(
    name="dob_timedelta",
    corruption_function=date_corrupt_timedelta,
    args={"input_colname": "dob", "output_colname": "dob", "num_days_delta": 50},
    baseline_probability=0.6,
)


# A corruption function that simultaneously sets dob and dod to jan first
# We will also add a probability adjustment that modifies the probability
# of activation based on the values in the record
dob_dod_jan_first = CompositeCorruption(
    name="dob_dod_jan_first_corruption", baseline_probability=0.1
)
dob_dod_jan_first.add_corruption_function(
    date_corrupt_jan_first, args={"input_colname": "dob", "output_colname": "dob"}
)
dob_dod_jan_first.add_corruption_function(
    date_corrupt_jan_first, args={"input_colname": "dod", "output_colname": "dod"}
)

rc.add_composite_corruption(dob_dod_jan_first)

# Make this twice as likely if the dob is < 1990
sql_condition = "year(cast(dob as date)) < 1900"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, dob_dod_jan_first, 4)
rc.add_probability_adjustment(adjustment)


########
# Name-based corruptions
########

rc.add_simple_corruption(
    name="pick_alt_name",
    corruption_function=full_name_alternative,
    args={},
    baseline_probability=0.2,
)




max_corrupted_records = 10
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
    uncorrupted_output_record["corruptions_applied"] = []

    # import pprint

    # pprint.pprint(formatted_master_record, indent=4)
    # print("--")
    # print("--")
    # print("--")
    # pprint.pprint(uncorrupted_output_record)
    # break

    output_records.append(uncorrupted_output_record)

    # How many corrupted records to generate
    total_num_corrupted_records = np.random.choice(
        zipf_dist["vals"], p=zipf_dist["weights"]
    )
    for i in range(total_num_corrupted_records):
        record_to_modify = uncorrupted_output_record.copy()
        record_to_modify["corruptions_applied"] = []
        record_to_modify["id"] = uncorrupted_output_record["cluster"] + f"_{i+1}"
        record_to_modify["uncorrupted_record"] = False
        rc.apply_probability_adjustments(uncorrupted_output_record)
        corrupted_record = rc.apply_corruptions_to_record(
            formatted_master_record,
            record_to_modify,
        )
        output_records.append(corrupted_record)

df = pd.DataFrame(output_records)

df.head(20)
