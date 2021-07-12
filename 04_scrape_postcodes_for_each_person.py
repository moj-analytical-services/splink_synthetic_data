import pandas as pd

import os
import glob

from scrape_wikidata.cleaning_fns import postcode_lookup_from_cleaned_person_data

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 10

files = sorted(
    glob.glob("scrape_wikidata/processed_data/step_1_one_line_per_person/*.parquet")
)

for i, f in enumerate(files):

    base = os.path.basename(f)
    out_path = f"scrape_wikidata/processed_data/step_2_person_postcode_lookups/{base}"

    if not os.path.exists(out_path):

        df = pd.read_parquet(f)
        pcs = postcode_lookup_from_cleaned_person_data(df)

        pcs.to_parquet(
            out_path,
            index=False,
        )
