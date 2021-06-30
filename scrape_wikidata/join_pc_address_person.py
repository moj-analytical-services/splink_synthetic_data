# %%
import pandas as pd
import duckdb

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000

# %%
person = pd.read_parquet(
    "processed_data/step_1_one_line_per_person/page000_0_to_2000.parquet"
)
pc = pd.read_parquet(
    "processed_data/step_2_person_postcode_lookups/page000_0_to_2000.parquet"
)
# %%

import duckdb

query = """
select * from
(SELECT *, row_number() over (partition by postcode) as row_num
FROM 'price_paid_address_only.parquet') as a
where row_num = 1


"""
one_address_per_postcode = duckdb.query(query).to_df()
# %%

len(one_address_per_postcode)
# %%
one_address_per_postcode.to_parquet("one_address_per_postcode.parquet", index=False)
# %%
pc_exp = pc.explode("nearby_postcodes")

query = """
select * from
(SELECT *, row_number() over (partition by person) as row_num
FROM pc_exp) as a
where row_num = 1


"""
one_postcode_per_person = duckdb.query(query).to_df()
# %%
one_postcode_per_person
# %%
query = """
select *

from person as p

left join one_postcode_per_person as pc
on p.human = pc.person

left join one_address_per_postcode as a

on pc.nearby_postcodes = a.postcode

"""

s = duckdb.query(query).to_df()
# %%

query = """
select
    human as person_id,

    humanlabel as full_name,
    substr(dob, 1,10) as dob,
    humanaltlabel as alt_full_names,
    given_namelabel as forename,
    family_namelabel as surname,

    place_birthlabel as place_birth,
    birth_countrylabel as birth_country,
    sex_or_genderlabel as gender,
    pseudonym as pseudonym,
    flat_unit_saon as flat_unit,
    house_number_paon as house_number,
    street,
    locality,
    town_city,
    district,
    county,
    postcode,
    humandescription as person_desc




from s
limit 20

"""
final = duckdb.query(query).to_df()

# %%
final.to_csv("sample_data.csv", index=False)
# %%
