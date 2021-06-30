# %%
import pandas as pd
import duckdb
import random
import numpy as np
import os
import glob

from scrape_wikidata.cleaning_fns import postcode_lookup_from_cleaned_person_data


pd.options.display.max_columns = 1000
pd.options.display.max_rows = 10


# %%
files = sorted(glob.glob("processed_data/step_1_one_line_per_person/*.parquet"))


# %%
for i, f in enumerate(files[:2]):

    df = pd.read_parquet(f)
    pcs = postcode_lookup_from_cleaned_person_data(df)
    base = os.path.basename(f)
    pcs.to_parquet(f"processed_data/step_2_person_postcode_lookups/{base}")


# %%
# curl -OL "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
