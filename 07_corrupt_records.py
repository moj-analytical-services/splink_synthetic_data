# %%
%load_ext autoreload
%autoreload 2
# display = print


import pandas as pd
import numpy as np


from corrupt.corruption_functions import master_record_no_op, basic_null_fn

from corrupt.dob_corrupt import dob_exact_match, dob_corrupt_typo

from corrupt.location_corrupt import (
    location_master_record,
    corrupt_location,
    location_exact_match,
    location_null,
    birth_place_master_record,
    birth_place_exact_match,
    corrupt_birth_place
)

from corrupt.gender_corrupt import gender_exact_match, gender_corrupt

from corrupt.name_corrupt import (
    full_name_exact_match,
    corrupt_full_name,
    full_name_null,
)


from corrupt.geco_corrupt import get_zipf_dist

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 200


zipf_dist = get_zipf_dist(4)


cc = [
    {
        "col_name": "full_name",
        "m_probabilities": [0.5, 0.5],
        "choose_master_data": master_record_no_op,
        "corruption_functions": [
            corrupt_full_name,
            full_name_exact_match,
        ],
        "null_function": full_name_null,
        "null_probability": 0.05,
    },
    {
        "col_name": "dob",
        "m_probabilities": [0.3, 0.7],
        "choose_master_data": master_record_no_op,
        "corruption_functions": [
            dob_corrupt_typo,
            dob_exact_match,
        ],
        "null_function": basic_null_fn("dob"),
        "null_probability": 0.2,
    },
       {
        "col_name": "birth_place",
        "m_probabilities": [0.9, 0.1],
        "choose_master_data": birth_place_master_record,
        "corruption_functions": [
            corrupt_birth_place,
            birth_place_exact_match,
        ],
        "null_function": basic_null_fn("birth_place"),
        "null_probability": 0.2,
    },
    {
        "col_name": "location",
        "m_probabilities": [0.5, 0.5],
        "choose_master_data": location_master_record,
        "corruption_functions": [
            corrupt_location,
            location_exact_match,
        ],
        "null_function": location_null,
        "null_probability": 0.2,
    },
    {
        "col_name": "gender",
        "m_probabilities": [0.01, 0.99],
        "choose_master_data": master_record_no_op,
        "corruption_functions": [
            gender_corrupt,
            gender_exact_match,
        ],
        "null_function": basic_null_fn("gender"),
        "null_probability": 0.2,
    },
]


# df = pd.read_parquet("scrape_wikidata/clean_data/master_data")
# df.sample(50).to_parquet("temp_delete_master_sample.parquet")
df5 = pd.read_parquet("temp_delete_master_sample.parquet")
df5 = df5.sample(5)
# df5 = df[df["human"].isin(["Q16193339", "Q6223445", "Q20942797"])]
# df5 = df[df["human"].isin(["Q20942797"])]


records = df5.to_dict(orient="records")


corrupted_records = []
for i, master_record in enumerate(records):

    uncorrupted_record = {"num_corruptions": 0, "uncorrupted_record": True}
    uncorrupted_record["id"] = master_record["human"]

    for c in cc:
        master_record = c["choose_master_data"](master_record)
        # display(pd.DataFrame([master_record]))
        fn = c["corruption_functions"][-1]
        uncorrupted_record = fn(master_record, uncorrupted_record)

    corrupted_records.append(uncorrupted_record)

    num_corrupted_records = np.random.choice(zipf_dist["vals"], p=zipf_dist["weights"])

    for i in range(num_corrupted_records):
        corrupted_record = {"num_corruptions": 0}
        corrupted_record["id"] = master_record["human"]
        for c in cc:
            weights = c["m_probabilities"]
            fns = c["corruption_functions"]
            fn = np.random.choice(fns, p=weights)

            corrupted_record = fn(master_record, corrupted_record)
            corrupted_record["uncorrupted_record"] = False

            p = c["null_probability"]
            fn = c["null_function"]
            corrupted_record = fn(master_record, null_prob=p, corrupted_record=corrupted_record)

        corrupted_records.append(corrupted_record)

corrupted_df = pd.DataFrame(corrupted_records)
for h in df5["human"]:
    print(("-" * 80).join(["\n"] * 6))
    display(df5[df5["human"] == h])
    display(corrupted_df[corrupted_df["id"] == h])


# %%

# %%
