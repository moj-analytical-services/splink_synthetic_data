import random
import pandas as pd
import numpy as np

import duckdb
import pyarrow.parquet as pq


from corrupt.corruption_functions import (
    master_record_no_op,
    basic_null_fn,
    scale_linear,
    initiate_counters,
)


from corrupt.corrupt_occupation import (
    occupation_format_master_record,
    occupation_gen_uncorrupted_record,
    occupation_corrupt,
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
        "col_name": "occupation",
        "format_master_data": occupation_format_master_record,
        "gen_uncorrupted_record": occupation_gen_uncorrupted_record,
        "corruption_functions": [{"fn": occupation_corrupt, "p": 1.0}],
        "null_function": basic_null_fn("occupation"),
        "start_prob_corrupt": 0.1,
        "end_prob_corrupt": 0.7,
        "start_prob_null": 0.0,
        "end_prob_null": 0.5,
    }
]


base_path = "out_data/wikidata/raw/persons/by_dob"
arrow_table = pq.read_table(base_path)

con = duckdb.connect(":memory:")
con.register("df", arrow_table)
sql = """
with a as (
select cast(dod[1] as date) as dod_date, *
from 'tidied.parquet'
)
select * exclude dod_date from a
where dod_date = '1993-11-01'

"""


raw_data = con.execute(sql).df()


max_corrupted_records = 20
zipf_dist = get_zipf_dist(max_corrupted_records)

records = raw_data.to_dict(orient="records")

records[0]


corrupted_records = []


def format_master_data(master_input_record, config):
    for c in config:
        fn = c["format_master_data"]
        master_record = fn(master_input_record)
    return master_record


def generate_uncorrupted_output_record(formatted_master_record, config):

    uncorrupted_record = {"uncorrupted_record": True}
    uncorrupted_record = initiate_counters(uncorrupted_record)

    uncorrupted_record["id"] = formatted_master_record["human"]

    for c in config:
        fn = c["gen_uncorrupted_record"]
        uncorrupted_record = fn(formatted_master_record, uncorrupted_record)

    return uncorrupted_record


def get_null_prob(counter, group_size, config):
    null_domain = [0, group_size - 1]
    null_range = [config["start_prob_null"], config["end_prob_null"]]
    null_scale = scale_linear(null_domain, null_range)
    prob_null = null_scale(counter)
    return prob_null


def get_prob_of_corruption(counter, group_size, config):
    prob_corrupt_domain = [0, group_size - 1]
    prob_corrupt_range = [config["start_prob_corrupt"], config["end_prob_corrupt"]]
    prob_corrupt_scale = scale_linear(prob_corrupt_domain, prob_corrupt_range)
    prob_corrupt = prob_corrupt_scale(counter)
    return prob_corrupt


def choose_corruption_function(config):
    weights = [f["p"] for f in config["corruption_functions"]]
    fns = [f["fn"] for f in config["corruption_functions"]]
    return np.random.choice(fns, p=weights)


def generate_corrupted_output_records(
    formatted_master_record,
    counter,
    total_num_corrupted_records,
    config,
):

    corrupted_record = {
        "num_corruptions": 0,
        "uncorrupted_record": False,
    }

    corrupted_record = initiate_counters(corrupted_record)
    corrupted_record["id"] = formatted_master_record["human"]

    for c in config:

        corrupted_record["uncorrupted_record"] = False

        prob_null = get_null_prob(counter, total_num_corrupted_records, c)
        prob_corrupt = get_prob_of_corruption(counter, total_num_corrupted_records, c)

        # Choose corruption function to apply
        corruption_function = choose_corruption_function(c)

        if random.uniform(0, 1) < prob_corrupt:
            fn = corruption_function
        else:
            fn = c["gen_uncorrupted_record"]

        corrupted_record = fn(formatted_master_record, corrupted_record)

        null_fn = c["null_function"]
        corrupted_record = null_fn(
            formatted_master_record,
            null_prob=prob_null,
            corrupted_record=corrupted_record,
        )

    return corrupted_record


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
df[select]
