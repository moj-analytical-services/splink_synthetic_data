import duckdb
import pyarrow.parquet as pq

base_path = "out_data/wikidata/raw/persons/by_dob"
arrow_table = pq.read_table(base_path)

con = duckdb.connect(":memory:")
con.register("df", arrow_table)

wikireplace = """replace({col}, 'http://www.wikidata.org/entity/', '') as {col}"""

sql = f"""
with nowikiurl as
(
select
    {wikireplace.format(col="human")},
    dod,
    {wikireplace.format(col="family_name")},
    dob,
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
)

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

"""

# where cast(dod[1] as date) = '2018-11-02'

tidied = con.execute(sql)

tidied_arrow = tidied.fetch_arrow_table()

pq.write_table(tidied_arrow, "tidied.parquet")

con.execute("""
select count(*)
from 'tidied.parquet'
""").df()
