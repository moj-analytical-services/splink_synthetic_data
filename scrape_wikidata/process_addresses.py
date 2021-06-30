# https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
# curl -OL "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
# %%
import pandas as pd

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

df = pd.read_csv("pp-complete.csv", header=None, names=cols)

# %%
df.head(2)
# %%

df.to_parquet("price_paid_complete.parquet", index=False)
# %%
df = pd.read_parquet("price_paid_address_only.parquet")
print(len(df))
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


df2.to_parquet("price_paid_address_only.parquet", index=False)
# %%
