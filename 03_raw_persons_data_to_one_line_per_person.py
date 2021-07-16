import pandas as pd

from math import ceil
import numpy as np
from scrape_wikidata.cleaning_fns import dedupe_and_clean_results

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000


df = pd.read_parquet("scrape_wikidata/raw_data/persons/")
df_clean = dedupe_and_clean_results(df)
df_clean["row_num"] = np.arange(len(df_clean))

df_random_postcode = pd.read_parquet("scrape_wikidata/raw_data/random_postcodes/flat")
df_random_postcode["random_coordinates"] = (
    "Point("
    + df_random_postcode["lng"].astype(str)
    + " "
    + df_random_postcode["lat"].astype(str)
    + ")"
)
df_random_postcode = df_random_postcode[["random_coordinates"]]
df_clean = df_clean.join(df_random_postcode)

print(len(df))
print(len(df_clean))

psize = 2000
n_partition = ceil(len(df_clean) / psize)
df_clean["partition_idx"] = np.floor(df_clean["row_num"] / psize)

for i in range(n_partition):
    this_df = df_clean[df_clean["partition_idx"] == i]
    this_df = this_df.drop("partition_idx", axis=1)
    this_df.to_parquet(
        f"scrape_wikidata/processed_data/step_1_one_line_per_person/page{i:03}_{i*psize}_to_{(i+1)*psize}.parquet",
        index=False,
    )
