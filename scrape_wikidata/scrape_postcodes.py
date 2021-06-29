# %%
import pandas as pd
import duckdb
import random
import numpy as np
from scrape_wikidata.postcodes import Api

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 10

# %%

df = pd.read_parquet("scrape_wikidata/processed_data/step_1/sample.parquet")


df["points"] = list(
    df[["human", "birth_coordinates", "residence_coordinates"]].itertuples(
        index=False, name=None
    )
)

cols = ["human", "points", "birth_coordinates", "residence_coordinates"]

df = df[cols].sample(20)
df
# %%
df2 = df
# %%


def points_to_array(x):
    h = x[0]
    p1 = x[1]
    if p1:
        p1 = p1.split(" | ")
    else:
        p1 = []
    p2 = x[2]
    if p2:
        p2 = p2.split(" | ")
    else:
        p2 = []

    p1.extend(p2)

    return [{"human": h, "point": p} for p in p1]


df2["point_array"] = df2["points"].map(points_to_array)

df_exploded = df2[["point_array"]].explode("point_array")
df_exploded = df_exploded[["point_array"]].dropna()


# %%
group_size = 10
df_exploded["group"] = np.floor(np.arange(len(df_exploded)) / group_size)


point_lists = df_exploded.groupby("group")["point_array"].apply(list).reset_index()

point_lists

# %%

api = Api()


def map_lat_lng(points_with_person, perturb=False):
    points = [p["point"] for p in points_with_person]
    persons = [p["human"] for p in points_with_person]

    if perturb:
        lat_lng_arr = [point_to_perturbed_lat_lng(p) for p in points]
    else:
        lat_lng_arr = [point_to_lat_lng(p) for p in points]
    payload = {"geolocations": lat_lng_arr}
    response = api.get_bulk_reverse_geocode(payload)
    response_array = response["result"]
    keys = ["point", "person", "pc_response"]
    zipped_list = list(zip(points, persons, response_array))
    list_of_dicts = [
        {"point": i[0], "person": i[1], "pc_response": i[2]} for i in zipped_list
    ]
    return list_of_dicts


def point_to_lat_lng(point_text, limit=1):
    lng, lat = point_text.replace("Point(", "").replace(")", "").split(" ")
    lng = float(lng)
    lat = float(lat)
    return {
        "longitude": lng,
        "latitude": lat,
        "limit": limit,
    }


def point_to_perturbed_lat_lng(point_text, limit=1):
    # Perturtabtion of about total 0.03
    lng, lat = point_text.replace("Point(", "").replace(")", "").split(" ")
    lng = float(lng) + random.uniform(-0.03, 0.03)
    lat = float(lat) + random.uniform(-0.03, 0.03)

    return {"longitude": lng, "latitude": lat, "limit": limit}


def get_postcode(x):
    if x:
        if x["result"]:
            return x["result"][0]["postcode"]
    else:
        return None


point_lists["geo_array"] = point_lists["point_array"].apply(map_lat_lng, perturb=False)
exploded = point_lists["geo_array"].explode("geo_array")
df_results_1 = pd.DataFrame(list(exploded))
df_results_1["postcode"] = df_results_1["pc_response"].apply(get_postcode)
df_results_1 = df_results_1[["point", "person", "postcode"]]

point_lists["geo_array"] = point_lists["point_array"].apply(map_lat_lng, perturb=True)
exploded = point_lists["geo_array"].explode("geo_array")
df_results_2 = pd.DataFrame(list(exploded))
df_results_2["postcode"] = df_results_2["pc_response"].apply(get_postcode)
df_results_2 = df_results_2[["point", "person", "postcode"]]

# %%


# %%
