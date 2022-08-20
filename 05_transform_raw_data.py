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

# Add columns to `raw_scraped_one_row_per_person.parquet` like `full_name_arr`
# That contains an array of distinct alternative full names


Path(TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON).mkdir(parents=True, exist_ok=True)


con = duckdb.connect()
pipeline = SQLPipeline(con)

sql = f"""
select *
from '{PERSONS_PROCESSED_ONE_ROW_PER_PERSON}'
"""

pipeline.enqueue_sql(sql, "df")


sql = f"""
select
    list_transform(birth_coordinates, x -> replace(replace(x, 'Point(', ''), ')', ''))
        as __coordinates
from df
limit 5 offset 2
"""
pipeline.enqueue_sql(sql, "space_delimited_coordinates")


sql = f"""
select
    list_transform(__coordinates, x -> str_split(x, ' '))
        as __coordinates
from space_delimited_coordinates
limit 5 offset 2
"""
pipeline.enqueue_sql(sql, "space_delimited_coordinates_2")


sql = f"""
select
    list_transform(__coordinates, x -> struct_pack(lat :=x[2], lng:=x[1])) as struct
from space_delimited_coordinates_2
"""

pipeline.enqueue_sql(sql, "done")

pipeline.execute_pipeline_in_parts()

# def point_to_lat_lng(point_text, limit=1):
#     lng, lat = point_text.replace("Point(", "").replace(")", "").split(" ")
#     lng = float(lng)
#     lat = float(lat)
#     return {"longitude": lng, "latitude": lat, "limit": limit, "radius": 1000}


add_full_name_alternatives_per_person(pipeline=pipeline)

df = pipeline.execute_pipeline()
out_path = os.path.join(
    TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON, "transformed_master_data.parquet"
)

df_arrow = df.fetch_arrow_table()
pq.write_table(df_arrow, out_path)

# Location tests
import pandas as pd

pd.options.display.max_columns = 1000
import duckdb
