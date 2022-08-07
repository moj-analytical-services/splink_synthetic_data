
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
pd.options.display.max_columns = 1000

base_path = "out_data/wikidata/raw/persons/by_dob"
arrow_table = pq.read_table(base_path)


import duckdb
con = duckdb.connect(":memory:")
con.register("df", arrow_table)

sql = """
select count(distinct human)
from df
"""
con.execute(sql).df()

sql = """
select
    human,
    humanLabel,
    name_native_language,
    birth_name,
    dob,
    given_nameLabel,
    family_nameLabel,
    humanDescription,
    place_birthLabel,
    occupationLabel,
    birth_coordinates,
    country_citizenLabel,
    sex_or_genderLabel,
    birth_countryLabel,
    ethnicity,
    ethnicityLabel,
    humanAltLabel,

    dod,
from df
where name_native_language is not null
USING SAMPLE 5% (bernoulli)
limit 10
"""

con.execute(sql).df()


