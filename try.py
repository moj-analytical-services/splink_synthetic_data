from os import replace
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

pd.options.display.max_columns = 1000


import duckdb

con = duckdb.connect(":memory:")


con.execute(
    """
select *
from 'tidied.parquet'
limit 10
"""
).df()
