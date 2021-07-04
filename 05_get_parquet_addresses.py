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
