import os
import pandas as pd
import numpy as np

import duckdb

from corrupt.corrupt_string import string_corrupt_numpad

from corrupt.corruption_functions import (
    master_record_no_op,
    basic_null_fn,
    format_master_data,
    generate_uncorrupted_output_record,
    generate_corrupted_output_records,
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

from corrupt.corrupt_dob import (
    dob_format_master_record,
)

from corrupt.corrupt_date import (
    date_corrupt_timedelta,
    date_corrupt_typo,
    date_gen_uncorrupted_record,
)

from path_fns.filepaths import TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON

from corrupt.corrupt_lat_lng import lat_lng_uncorrupted_record, lat_lng_corrupt_distance

from functools import partial
from corrupt.geco_corrupt import get_zipf_dist


# Configure how corruptions will be made for each field

# Col name is the OUTPUT column name.  For instance, we may input given name,
# family name etc to output full_name

# Guide to keys:
# format_master_data.  This functino may apply additional cleaning to the master
# record.  The same formatted master ata is then available to the
# 'gen_uncorrupted_record' and 'corruption_functions'


# Finally, as we generate more duplicate records, we introduce more and more errors.
# The keys start_prob_corrupt, end_prob_corrupt, start_prob_null, end_prob_null control
# the probability of corruption

config = [
    {
        "col_name": "full_name",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": full_name_gen_uncorrupted_record,
        "corruption_functions": [
            {"fn": full_name_alternative, "p": 0.5},
            {"fn": each_name_alternatives, "p": 0.4},
            {"fn": full_name_typo, "p": 0.1},
        ],
        "null_function": full_name_null,
        "start_prob_corrupt": 0.1,
        "end_prob_corrupt": 0.6,
        "start_prob_null": 0.0,
        "end_prob_null": 0.5,
    },
    {
        "col_name": "occupation",
        "format_master_data": occupation_format_master_record,
        "gen_uncorrupted_record": occupation_gen_uncorrupted_record,
        "corruption_functions": [{"fn": occupation_corrupt, "p": 1.0}],
        "null_function": basic_null_fn("occupation"),
        "start_prob_corrupt": 0.1,
        "end_prob_corrupt": 0.7,
        "start_prob_null": 0.0,
        "end_prob_null": 0.5,
    },
    {
        "col_name": "dob",
        "format_master_data": partial(
            format_master_record_first_array_item, colname="dob"
        ),
        "gen_uncorrupted_record": partial(
            date_gen_uncorrupted_record, input_colname="dob", output_colname="dob"
        ),
        "corruption_functions": [
            {
                "fn": partial(
                    date_corrupt_timedelta, input_colname="dob", output_colname="dob"
                ),
                "p": 0.7,
            },
            {
                "fn": partial(
                    string_corrupt_numpad, input_colname="dob", output_colname="dob"
                ),
                "p": 0.3,
            },
        ],
        "null_function": basic_null_fn("dob"),
        "start_prob_corrupt": 1.0,
        "end_prob_corrupt": 1.0,
        "start_prob_null": 0.0,
        "end_prob_null": 0.0,
    },
    {
        "col_name": "birth_coordinates",
        "format_master_data": partial(
            format_master_record_first_array_item, colname="birth_coordinates"
        ),
        "gen_uncorrupted_record": partial(
            lat_lng_uncorrupted_record,
            input_colname="birth_coordinates",
            output_colname="birth_coordinates",
        ),
        "corruption_functions": [
            {
                "fn": partial(
                    lat_lng_corrupt_distance,
                    input_colname="birth_coordinates",
                    output_colname="birth_coordinates",
                    distance_min=0.1,
                    distance_max=10,
                ),
                "p": 1.0,
            },
        ],
        "null_function": basic_null_fn("birth_coordinates"),
        "start_prob_corrupt": 1.0,
        "end_prob_corrupt": 1.0,
        "start_prob_null": 0.0,
        "end_prob_null": 0.0,
    },
]


con = duckdb.connect()

in_path = os.path.join(
    TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON, "transformed_master_data.parquet"
)


sql = f"""
select *
from '{in_path}'
limit 500
"""

pd.options.display.max_columns = 1000
raw_data = con.execute(sql).df()
raw_data


max_corrupted_records = 20
zipf_dist = get_zipf_dist(max_corrupted_records)

records = raw_data.to_dict(orient="records")


corrupted_records = []


output_records = []
for i, master_input_record in enumerate(records):

    # Formats the input data into an easy format for producing
    # an uncorrupted/corrupted outputs records
    formatted_master_record = format_master_data(master_input_record, config)

    uncorrupted_output_record = generate_uncorrupted_output_record(
        formatted_master_record, config
    )

    output_records.append(uncorrupted_output_record)
    total_num_corrupted_records = np.random.choice(
        zipf_dist["vals"], p=zipf_dist["weights"]
    )

    for counter in range(total_num_corrupted_records):
        corrupted_output_record = generate_corrupted_output_records(
            formatted_master_record,
            counter,
            total_num_corrupted_records,
            config,
        )
        output_records.append(corrupted_output_record)

df = pd.DataFrame(output_records)

df.head(20)
