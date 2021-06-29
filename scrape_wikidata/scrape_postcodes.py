# %%
import pandas as pd
import duckdb
import random
import numpy as np

from scrape_wikidata.cleaning_fns import postcode_lookup_from_cleaned_person_data


pd.options.display.max_columns = 1000
pd.options.display.max_rows = 10


df = pd.read_parquet("scrape_wikidata/processed_data/step_1/sample.parquet")

pcs = postcode_lookup_from_cleaned_person_data(df.head(20))
pcs

# %%

# df["points"] = list(
#     df[["human", "birth_coordinates", "residence_coordinates"]].itertuples(
#         index=False, name=None
#     )
# )

# cols = ["human", "points", "birth_coordinates", "residence_coordinates"]

# df = df[cols].sample(20)
# df
# # %%
# df2 = df
# # %%


# df2["point_array"] = df2["points"].map(points_to_array)

# df_exploded = df2[["point_array"]].explode("point_array")
# df_exploded = df_exploded[["point_array"]].dropna()


# # %%
# group_size = 10
# df_exploded["group"] = np.floor(np.arange(len(df_exploded)) / group_size)


# point_lists

# # %%

# api = Api()


# point_lists["geo_array"] = point_lists["point_array"].apply(map_lat_lng, perturb=False)
# exploded = point_lists["geo_array"].explode("geo_array")
# df_results_1 = pd.DataFrame(list(exploded))
# df_results_1["postcode"] = df_results_1["pc_response"].apply(get_postcode)
# df_results_1 = df_results_1[["point", "person", "postcode"]]

# point_lists["geo_array"] = point_lists["point_array"].apply(map_lat_lng, perturb=True)
# exploded = point_lists["geo_array"].explode("geo_array")
# df_results_2 = pd.DataFrame(list(exploded))
# df_results_2["postcode"] = df_results_2["pc_response"].apply(get_postcode)
# df_results_2 = df_results_2[["point", "person", "postcode"]]

# # %%
# sql = """
# select a.person, a.point, a.postcode as postcode, b.postcode as pc_pert from
# df_results_1 as a
# left join
# df_results_2 as b
# on a.point = b.point and a.person=b.person
# order by point

# """
# duckdb.query(sql).to_df().head(10)

# # %%
