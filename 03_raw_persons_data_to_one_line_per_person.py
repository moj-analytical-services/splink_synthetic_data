import duckdb
import pyarrow.parquet as pq

from path_fns.filepaths import (
    PERSONS_BY_DOB_RAW_OUT_PATH,
    PERSONS_PROCESSED_ONE_ROW_PER_PERSON,
)

# Using arrow to read because it performs schema merging
# i.e. if some files are missing a column it doesn't matter
# duckdb doesn't do this
arrow_table = pq.read_table(PERSONS_BY_DOB_RAW_OUT_PATH)

con = duckdb.connect(":memory:")
con.register("df", arrow_table)

wikireplace = """replace({col}, 'http://www.wikidata.org/entity/', '') as {col}"""
cast_date = "TRY_CAST({col} as date) as {col}"

sql = f"""
COPY (
with nowikiurl as
(
select
    {wikireplace.format(col="human")},
    {cast_date.format(col="dod")},
    {wikireplace.format(col="family_name")},
    {cast_date.format(col="dob")},
    {wikireplace.format(col="given_name")},
    {wikireplace.format(col="country_citizen")},
    {wikireplace.format(col="occupation")},
    humanLabel,
    given_nameLabel,
    family_nameLabel,
    occupationLabel,
    country_citizenLabel,
    sex_or_genderLabel,
    {wikireplace.format(col="place_birth")},
    birth_coordinates,
    {wikireplace.format(col="birth_country")},
    place_birthLabel,
    birth_countryLabel,
    birth_name,
    humanDescription,
    name_native_language,
    humanAltLabel,
    {wikireplace.format(col="residence")},
    residence_coordinates,
    residenceLabel,
    residence_countryLabel,
    pseudonym,
    {wikireplace.format(col="ethnicity")},
    ethnicityLabel,
from df

),
distinct_arrays as (
select
    human,
    list(distinct dod) as dod,
    list(distinct family_name) as family_name,
    list(distinct dob) as dob,
    list(distinct given_name) as given_name,
    list(distinct country_citizen) as country_citizen,
    list(distinct occupation) as occupation,
    list(distinct humanLabel) as humanLabel,
    list(distinct given_nameLabel) as given_nameLabel,
    list(distinct family_nameLabel) as family_nameLabel,
    list(distinct occupationLabel) as occupationLabel,
    list(distinct country_citizenLabel) as country_citizenLabel,
    list(distinct sex_or_genderLabel) as sex_or_genderLabel,
    list(distinct place_birth) as place_birth,
    list(distinct birth_coordinates) as birth_coordinates,
    list(distinct birth_country) as birth_country,
    list(distinct place_birthLabel) as place_birthLabel,
    list(distinct birth_countryLabel) as birth_countryLabel,
    list(distinct birth_name) as birth_name,
    list(distinct humanDescription) as humanDescription,
    list(distinct name_native_language) as name_native_language,
    list(distinct humanAltLabel) as humanAltLabel,
    list(distinct residence) as residence,
    list(distinct residence_coordinates) as residence_coordinates,
    list(distinct residenceLabel) as residenceLabel,
    list(distinct residence_countryLabel) as residence_countryLabel,
    list(distinct pseudonym) as pseudonym,
    list(distinct ethnicity) as ethnicity,
    list(distinct ethnicityLabel) as ethnicityLabel
from nowikiurl
group by human
)
select
    human,
    array_filter(dod, x -> x is not null) as dod,
    array_filter(family_name, x -> x is not null) as family_name,
    array_filter(dob, x -> x is not null) as dob,
    array_filter(given_name, x -> x is not null) as given_name,
    array_filter(country_citizen, x -> x is not null) as country_citizen,
    array_filter(occupation, x -> x is not null) as occupation,
    array_filter(humanLabel, x -> x is not null) as humanLabel,
    array_filter(given_nameLabel, x -> x is not null) as given_nameLabel,
    array_filter(family_nameLabel, x -> x is not null) as family_nameLabel,
    array_filter(occupationLabel, x -> x is not null) as occupationLabel,
    array_filter(country_citizenLabel, x -> x is not null) as country_citizenLabel,
    array_filter(sex_or_genderLabel, x -> x is not null) as sex_or_genderLabel,
    array_filter(place_birth, x -> x is not null) as place_birth,
    array_filter(birth_coordinates, x -> x is not null) as birth_coordinates,
    array_filter(birth_country, x -> x is not null) as birth_country,
    array_filter(place_birthLabel, x -> x is not null) as place_birthLabel,
    array_filter(birth_countryLabel, x -> x is not null) as birth_countryLabel,
    array_filter(birth_name, x -> x is not null) as birth_name,
    array_filter(humanDescription, x -> x is not null) as humanDescription,
    array_filter(name_native_language, x -> x is not null) as name_native_language,
    array_filter(humanAltLabel, x -> x is not null) as humanAltLabel,
    array_filter(residence, x -> x is not null) as residence,
    array_filter(residence_coordinates, x -> x is not null) as residence_coordinates,
    array_filter(residenceLabel, x -> x is not null) as residenceLabel,
    array_filter(residence_countryLabel, x -> x is not null) as residence_countryLabel,
    array_filter(pseudonym, x -> x is not null) as pseudonym,
    array_filter(ethnicity, x -> x is not null) as ethnicity,
    array_filter(ethnicityLabel, x -> x is not null) as ethnicityLabel
from distinct_arrays
)
TO '{PERSONS_PROCESSED_ONE_ROW_PER_PERSON}' (FORMAT 'parquet')

"""


con.execute(sql)


import pandas as pd

pd.options.display.max_columns = 1000

display(
    con.execute(
        f"""
select count(*)
from '{PERSONS_PROCESSED_ONE_ROW_PER_PERSON}'
"""
    ).df()
)

# USING SAMPLE 0.01% (bernoulli)
con.execute(
    f"""
select *
from '{PERSONS_PROCESSED_ONE_ROW_PER_PERSON}'



limit 1
"""
).df().T
