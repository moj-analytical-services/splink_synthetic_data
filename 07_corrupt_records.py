# %%
import pandas as pd
import numpy as np


from corrupt.corruption_functions import (
    country_citizenship_exact_match,
    country_citizenship_random,
    dob_exact_match,
    dob_corrupt_typo,
    birth_place_exact_match,
    residence_place_exact_match,
    gender_exact_match,
)

from corrupt.postcode_corrupt import (
    corrupt_location,
    location_exact_match,
)

from corrupt.name_corrupt import full_name_exact_match, corrupt_full_name


from corrupt.geco_corrupt import get_zipf_dist

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 200


zipf_dist = get_zipf_dist(4)


cc = [
    {
        "col_name": "full_name",
        "m_probabilities": [1.0, 0.0],
        "corruption_functions": [
            corrupt_full_name,
            full_name_exact_match,
        ],
    },
    {
        "col_name": "dob",
        "m_probabilities": [1.0, 0.0],
        "corruption_functions": [
            dob_corrupt_typo,
            dob_exact_match,
        ],
    },
    {
        "col_name": "location",
        "m_probabilities": [1.0, 0.0],
        "corruption_functions": [
            corrupt_location,
            location_exact_match,
        ],
    },
    {
        "col_name": "citizen",
        "m_probabilities": [1.0, 0.0],
        "corruption_functions": [
            country_citizenship_random,
            country_citizenship_exact_match,
        ],
    },
    {
        "col_name": "residence_place",
        "m_probabilities": [1.0, 0.0],
        "corruption_functions": [
            residence_place_exact_match,
            residence_place_exact_match,
        ],
    },
    {
        "col_name": "gender",
        "m_probabilities": [1.0, 0.0],
        "corruption_functions": [
            gender_exact_match,
            gender_exact_match,
        ],
    },
]


df = pd.read_parquet("scrape_wikidata/clean_data/master_data")
df5 = df.sample(5)
# df5 = df[df["human"].isin(["Q16193339", "Q6223445", "Q20942797"])]
# df5 = df[df["human"].isin(["Q20942797"])]


records = df5.to_dict(orient="records")
display(df5)

corrupted_records = []
for master_record in records:

    uncorrupted_record = {"num_corruptions": 0, "uncorrupted_record": True}
    for c in cc:
        fn = c["corruption_functions"][-1]
        uncorrupted_record = fn(master_record, uncorrupted_record)

    corrupted_records.append(uncorrupted_record)

    num_corrupted_records = np.random.choice(zipf_dist["vals"], p=zipf_dist["weights"])

    for i in range(num_corrupted_records):
        corrupted_record = {"num_corruptions": 0}
        for c in cc:
            weights = c["m_probabilities"]
            fns = c["corruption_functions"]
            fn = np.random.choice(fns, p=weights)

            corrupted_record = fn(master_record, corrupted_record)
            corrupted_record["uncorrupted_record"] = False

        corrupted_records.append(corrupted_record)

pd.DataFrame(corrupted_records)


# %%

# %%
