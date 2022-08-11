import duckdb
import pyarrow.parquet as pq
from pathlib import Path
import os


from transform_master_data.full_name_alternatives_per_person import (
    add_full_name_alternatives_per_person,
)
from transform_master_data.pipeline import SQLPipeline

# Add columns to `raw_scraped_one_row_per_person.parquet` like `full_name_arr`
# That contains an array of distinct alternative full names

out_folder = "out_data/wikidata/transformed_master_data/one_row_per_person/"
Path(out_folder).mkdir(parents=True, exist_ok=True)


con = duckdb.connect()
pipeline = SQLPipeline(con)


add_full_name_alternatives_per_person(pipeline=pipeline)

df = pipeline.execute_pipeline()
out_path = os.path.join(out_folder, "transformed_master_data.parquet")

df_arrow = df.fetch_arrow_table()
pq.write_table(df_arrow, out_path)
