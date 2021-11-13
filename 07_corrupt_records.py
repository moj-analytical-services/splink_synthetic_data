# %%
# %load_ext autoreload
# %autoreload 2
# display = print

import random
import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime

from corrupt.corruption_functions import (
    master_record_no_op,
    basic_null_fn,
    scale_linear,
    initiate_counters,
)

from corrupt.dob_corrupt import dob_exact_match, dob_corrupt_typo

from corrupt.location_corrupt import (
    location_master_record,
    location_exact_match,
    corrupt_location_move_house,
    corrupt_location_nearby_postcode_house,
    corrupt_location_postcode_typo,
    birth_place_master_record,
    birth_place_exact_match,
    corrupt_birth_place,
    location_null,
)

from corrupt.gender_corrupt import gender_exact_match, gender_corrupt

from corrupt.name_corrupt import (
    full_name_exact_match,
    corrupt_full_name,
    full_name_null,
)

from corrupt.corrupt_occupation import (
    occupation_master_record,
    occupation_exact_match,
    occupation_corrupt,
)

from corrupt.geco_corrupt import get_zipf_dist

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 200


cc = [
    {
        "col_name": "full_name",
        "choose_master_data": master_record_no_op,
        "exact_match": full_name_exact_match,
        "corruption_functions": [{"fn": corrupt_full_name, "p": 1.0}],
        "null_function": full_name_null,
        "start_prob_corrupt": 0.1,
        "end_prob_corrupt": 1.0,
        "start_prob_null": 0.0,
        "end_prob_null": 0.2,
    },
    {
        "col_name": "dob",
        "choose_master_data": master_record_no_op,
        "exact_match": dob_exact_match,
        "corruption_functions": [{"fn": dob_corrupt_typo, "p": 1.0}],
        "null_function": basic_null_fn("dob"),
        "start_prob_corrupt": 0.1,
        "end_prob_corrupt": 0.7,
        "start_prob_null": 0.0,
        "end_prob_null": 0.5,
    },
    {
        "col_name": "birth_place",
        "choose_master_data": birth_place_master_record,
        "exact_match": birth_place_exact_match,
        "corruption_functions": [{"fn": corrupt_birth_place, "p": 1.0}],
        "null_function": basic_null_fn("birth_place"),
        "start_prob_corrupt": 0.1,
        "end_prob_corrupt": 0.7,
        "start_prob_null": 0.0,
        "end_prob_null": 0.3,
    },
    {
        "col_name": "location",
        "choose_master_data": location_master_record,
        "exact_match": location_exact_match,
        "corruption_functions": [
            {"fn": corrupt_location_move_house, "p": 0.6},
            {"fn": corrupt_location_nearby_postcode_house, "p": 0.3},
            {"fn": corrupt_location_postcode_typo, "p": 0.1},
        ],
        "null_function": location_null,
        "start_prob_corrupt": 0.1,
        "end_prob_corrupt": 0.7,
        "start_prob_null": 0.0,
        "end_prob_null": 0.5,
    },
    {
        "col_name": "gender",
        "choose_master_data": master_record_no_op,
        "exact_match": gender_exact_match,
        "corruption_functions": [{"fn": gender_corrupt, "p": 1.0}],
        "null_function": basic_null_fn("gender"),
        "start_prob_corrupt": 0.01,
        "end_prob_corrupt": 0.1,
        "start_prob_null": 0.0,
        "end_prob_null": 0.5,
    },
    {
        "col_name": "occupation",
        "choose_master_data": occupation_master_record,
        "exact_match": occupation_exact_match,
        "corruption_functions": [{"fn": occupation_corrupt, "p": 1.0}],
        "null_function": basic_null_fn("occupation"),
        "start_prob_corrupt": 0.1,
        "end_prob_corrupt": 1.0,
        "start_prob_null": 0.0,
        "end_prob_null": 0.7,
    },
]


# df = pd.read_parquet("scrape_wikidata/clean_data/master_data")
# df.sample(50).to_parquet("temp_delete_master_sample.parquet")
# df5 = pd.read_parquet("temp_delete_master_sample.parquet")
# df5 = df5.sample(2)
# df5 = df[df["human"].isin(["Q16193339", "Q6223445", "Q20942797"])]
# df5 = df[df["human"].isin(["Q20942797"])]


max_corrupted_records = 20
zipf_dist = get_zipf_dist(max_corrupted_records)


files = sorted(glob.glob("scrape_wikidata/clean_data/master_data/*.parquet"))

startTime = datetime.now()
for f in files:

    basename = os.path.basename(f)
    out_path = os.path.join("corrupted_data/uk_citizens_groupsize_20", basename)

    if os.path.exists(out_path):
        continue

    print(f"Starting {f}")
    print(f"Time taken {datetime.now() - startTime}")

    df = pd.read_parquet(f)
    print(f"Inputted {len(df)} records")
    records = df.to_dict(orient="records")

    corrupted_records = []
    for i, master_record in enumerate(records):

        if master_record["random_nearby_locs"] is None:
            human = master_record["human"]
            print(f"No random location for {human}, ignoring")
            continue

        uncorrupted_record = {"uncorrupted_record": True}
        uncorrupted_record = initiate_counters(uncorrupted_record)
        uncorrupted_record["id"] = master_record["human"]

        for c in cc:
            master_record = c["choose_master_data"](master_record)
            fn = c["exact_match"]
            uncorrupted_record = fn(master_record, uncorrupted_record)

        corrupted_records.append(uncorrupted_record)

        num_corrupted_records = np.random.choice(
            zipf_dist["vals"], p=zipf_dist["weights"]
        )

        for corruption_number in range(num_corrupted_records):
            corrupted_record = {
                "num_corruptions": 0,
                "uncorrupted_record": False,
            }

            corrupted_record = {"uncorrupted_record": False}
            corrupted_record = initiate_counters(corrupted_record)

            corrupted_record["id"] = master_record["human"]
            for c in cc:

                null_domain = [0, num_corrupted_records - 1]
                null_range = [c["start_prob_null"], c["end_prob_null"]]
                null_scale = scale_linear(null_domain, null_range)
                prob_null = null_scale(corruption_number)

                prob_corrupt_domain = [0, num_corrupted_records - 1]
                prob_corrupt_range = [c["start_prob_corrupt"], c["end_prob_corrupt"]]
                prob_corrupt_scale = scale_linear(
                    prob_corrupt_domain, prob_corrupt_range
                )
                prob_corrupt = prob_corrupt_scale(corruption_number)

                weights = [f["p"] for f in c["corruption_functions"]]
                fns = [f["fn"] for f in c["corruption_functions"]]

                if random.uniform(0, 1) < prob_corrupt:
                    fn = np.random.choice(fns, p=weights)
                else:
                    fn = c["exact_match"]

                corrupted_record = fn(master_record, corrupted_record)
                corrupted_record["uncorrupted_record"] = False

                null_fn = c["null_function"]
                corrupted_record = null_fn(
                    master_record,
                    null_prob=prob_null,
                    corrupted_record=corrupted_record,
                )

            corrupted_records.append(corrupted_record)

    corrupted_df = pd.DataFrame(corrupted_records)

    num_cols = [c for c in corrupted_df.columns if c.startswith("num_")]
    non_num_cols = [c for c in corrupted_df.columns if not c.startswith("num_")]
    non_num_cols.extend(num_cols)
    corrupted_df = corrupted_df[non_num_cols]
    corrupted_df.to_parquet(out_path)
    print(f"outputted {len(corrupted_df)} records")

# reorder columns

# for h in df5["human"]:
#     print(("-" * 80).join(["\n"] * 6))
#     display(df5[df5["human"] == h])
#     display(corrupted_df[corrupted_df["id"] == h])

# %%

pd.read_parquet("corrupted_data/uk_citizens_groupsize_20/").head(20)
# %%
