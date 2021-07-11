# %%
%load_ext autoreload
%autoreload 2



# %%
import pandas as pd
import numpy as np

from corrupt.corruption_functions import (
    country_citizenship_exact_match,
    country_citizenship_random,
    dob_exact_match,
    dob_corrupt_typo,
    full_name_exact_match,
    full_name_use_alt_label_if_exists,
    full_name_use_alt_given_names,
    postcode_lat_lng_exact_match,
    postcode_lat_lng_alternative,
    birth_place_exact_match,
    residence_place_exact_match
)

from corrupt.geco_corrupt import get_zipf_dist

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 200


import stackprinter
stackprinter.set_excepthook(style='darkbg2')

# %%

df = pd.read_parquet("scrape_wikidata/clean_data/master_data")


# %%
# df5 = df.sample(5).copy()
zipf_dist = get_zipf_dist(4)


cc = [
    {
        "col_name": "first_name",
        "m_probabilities": [0.5, 0.5, 0.0],
        "corruption_functions": [
            full_name_use_alt_label_if_exists,
            full_name_use_alt_given_names,
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
            postcode_lat_lng_alternative,
            postcode_lat_lng_exact_match,
        ],
    },
    {
        "col_name": "citizen",
        "m_probabilities":  [1.0, 0.0],
        "corruption_functions": [
            country_citizenship_random,
            country_citizenship_exact_match,
        ],
    },
    {
        "col_name": "birth_place",
        "m_probabilities": [1.0, 0.0],
        "corruption_functions": [
            birth_place_exact_match,
            birth_place_exact_match,
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
]


records = df5.to_dict(orient="records")
display(df5)
corrupted_records = []
for master_record in records:

    uncorrupted_record = {}
    for c in cc:
        fn = fns = c["corruption_functions"][-1]
        uncorrupted_record = fn(master_record, uncorrupted_record)
    corrupted_records.append(uncorrupted_record)

    num_corrupted_records = np.random.choice(zipf_dist["vals"], p=zipf_dist["weights"])
    for i in range(num_corrupted_records):
        corrupted_record = {}
        for c in cc:
            weights = c["m_probabilities"]
            fns = c["corruption_functions"]
            fn = np.random.choice(fns, p=weights)

            corrupted_record = fn(master_record, corrupted_record)
            # corrupted_record[c["col_name"] + "_fn"] = fn.__name__

        corrupted_records.append(corrupted_record)

pd.DataFrame(corrupted_records)


# %%

# %%
