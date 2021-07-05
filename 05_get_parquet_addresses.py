# https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
# curl -OL "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
# %%
import pandas as pd
import duckdb

# %%
cols = [
    "uid",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "house_number_paon",
    "flat_unit_saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",
    "record_status",
]

df = pd.read_csv("raw_data/addresses/pp-complete.csv", header=None, names=cols)

# %%

# %%

df.to_parquet(
    "processed_data/step_5_addresses/price_paid_complete.parquet", index=False
)
# %%


retain = [
    "house_number_paon",
    "flat_unit_saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "postcode",
]

df2 = df[retain].drop_duplicates()
df2.loc[df2["locality"] == df2["town_city"], "locality"] = None
df2.loc[df2["district"] == df2["town_city"], "town_city"] = None
df2.loc[df2["district"] == df2["county"], "county"] = None
df2.loc[df2["district"] == df2["locality"], "locality"] = None


df2.to_parquet(
    "processed_data/step_5_addresses/price_paid_address_only.parquet", index=False
)


query = """
select * from
(SELECT *, row_number() over (partition by postcode) as row_num
FROM 'price_paid_address_only.parquet') as a



"""
postcode_address_with_rownum = duckdb.query(query).to_df()
postcode_address_with_rownum.to_parquet(
    "processed_data/step_5_addresses/postcode_address_with_rownum.parquet", index=False
)

# Use spark to get array of addresses from which to select
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df_addresses = spark.read.parquet(
    "scrape_wikidata/processed_data/step_5_addresses/postcode_address_with_rownum.parquet"
)
df_addresses.createOrReplaceTempView("df_addresses")

sql = """

select postcode, collect_list(to_json(struct(flat_unit_saon, house_number_paon, street, locality,town_city,district, county, postcode))) as address_array
from
df_addresses

group by postcode

"""

addresses_as_array = spark.sql(sql)
addresses_as_array.createOrReplaceTempView(f"addresses_as_array")
addresses_as_array = addresses_as_array.repartition(1)
addresses_as_array.write.mode("append").parquet(
    "scrape_wikidata/processed_data/step_5_addresses/addresses_as_array/"
)
