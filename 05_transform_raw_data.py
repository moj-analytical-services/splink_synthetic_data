import duckdb
from transform_master_data.pipeline import SQLPipeline
from transform_master_data.tokens_in_full_name_not_in_given_family_name import (
    tokens_in_full_name_not_in_given_family_name,
)


df = tokens_in_full_name_not_in_given_family_name()
df
