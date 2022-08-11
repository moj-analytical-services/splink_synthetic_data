import pandas as pd
import numpy as np

import duckdb


from corrupt.corruption_functions import (
    master_record_no_op,
    basic_null_fn,
    format_master_data,
    generate_uncorrupted_output_record,
    generate_corrupted_output_records,
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
    dob_gen_uncorrupted_record,
    dob_corrupt_typo,
    dob_corrupt_timedelta,
    dob_format_master_record,
)

from corrupt.geco_corrupt import get_zipf_dist


# Configure how corruptions will be made for each field

# Col name is the OUTPUT column name.  For instance, we may input given name, family name etc to output full_name

# Guide to keys:
# format_master_data.  A function that take the input master data - usually an array - and returns a single value.
# 'no_op' means no operation, i.e. reproduce the value from the master data.

# We then have an 'exact match' function, which controls how to render the master data into the output data.

# For instance, the master data for birth_place may use the person's birth place if it exists, but if it doesn't, may use a known place they lived.

# We then have a dictionary of 'corruption' functions, each with a probability (p) of being applied.
# This probability is conditional on the record being selected for corruption e.g. add a typo with probability 0.5, or alternatively use an alias with probability 0.5

# Finally, as we generate more duplicate records, we introduce more and more errors.
# The keys start_prob_corrupt, end_prob_corrupt, start_prob_null, end_prob_null control the probability of corruption

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
        "format_master_data": dob_format_master_record,
        "gen_uncorrupted_record": dob_gen_uncorrupted_record,
        "corruption_functions": [{"fn": dob_corrupt_timedelta, "p": 1.0}],
        "null_function": basic_null_fn("dob"),
        "start_prob_corrupt": 1.0,
        "end_prob_corrupt": 1.0,
        "start_prob_null": 0.0,
        "end_prob_null": 0.0,
    },
]


con = duckdb.connect()

sql = """
select *
from 'out_data/wikidata/transformed_master_data/one_row_per_person/transformed_master_data.parquet'
limit 500
"""


raw_data = con.execute(sql).df()
raw_data


max_corrupted_records = 20
zipf_dist = get_zipf_dist(max_corrupted_records)

records = raw_data.to_dict(orient="records")

records[0]


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
cols = list(df.columns)


# Remove columns for final output

num_cols = [c for c in cols if c.startswith("num_")]
other_cols = [c for c in cols if not c.startswith("num_") and c != "uncorrupted_record"]

select = other_cols + ["uncorrupted_record"] + num_cols

ids = list(df["id"].unique())
ids = np.random.choice(ids, 3, replace=False)
f = df["id"].isin(ids)
df[f][select]
