import duckdb
import pyarrow.parquet as pq
from pathlib import Path
import os
from path_fns.filepaths import (
    TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON,
    PERSONS_PROCESSED_ONE_ROW_PER_PERSON,
)


from transform_master_data.full_name_alternatives_per_person import (
    add_full_name_alternatives_per_person,
)
from transform_master_data.pipeline import SQLPipeline

from transform_master_data.parse_point import parse_point_to_lat_lng

Path(TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON).mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
pipeline = SQLPipeline(con)


sql = f"""
select *
from '{PERSONS_PROCESSED_ONE_ROW_PER_PERSON}'
"""

pipeline.enqueue_sql(sql, "df")

pipeline = add_full_name_alternatives_per_person(
    pipeline, output_table_name="df_full_names", input_table_name="df"
)

pipeline = parse_point_to_lat_lng(
    pipeline,
    "birth_coordinates",
    output_table_name="df_bc_fixed",
    input_table_name="df_full_names",
)
pipeline = parse_point_to_lat_lng(
    pipeline,
    "residence_coordinates",
    output_table_name="df_rc_fixed",
    input_table_name="df_bc_fixed",
)


df = pipeline.execute_pipeline()


out_path = os.path.join(
    TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON, "transformed_master_data.parquet"
)

df_arrow = df.fetch_arrow_table()
pq.write_table(df_arrow, out_path)
