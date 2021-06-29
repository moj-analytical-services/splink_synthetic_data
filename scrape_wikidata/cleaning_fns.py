# POSTCODE CLEANING FUNCTIONS

import random

import pandas as pd
import numpy as np
import duckdb

from scrape_wikidata.postcodes import Api


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

    p1 = list(set(p1))

    return [{"human": h, "point": p} for p in p1]


def map_lat_lng(points_with_person, perturb=False):
    api = Api()
    points = [p["point"] for p in points_with_person]
    persons = [p["human"] for p in points_with_person]

    if perturb:
        lat_lng_arr = [point_to_perturbed_lat_lng(p) for p in points]
    else:
        lat_lng_arr = [point_to_lat_lng(p) for p in points]
    payload = {"geolocations": lat_lng_arr}
    response = api.get_bulk_reverse_geocode(payload)
    response_array = response["result"]

    zipped_list = list(zip(points, persons, response_array))
    list_of_dicts = [
        {"point": i[0], "person": i[1], "pc_response": i[2]} for i in zipped_list
    ]
    return list_of_dicts


def point_to_lat_lng(point_text, limit=1):
    lng, lat = point_text.replace("Point(", "").replace(")", "").split(" ")
    lng = float(lng)
    lat = float(lat)
    return {"longitude": lng, "latitude": lat, "limit": limit, "radius": 1000}


def point_to_perturbed_lat_lng(point_text, limit=5):
    # Perturtabtion of about total 0.03
    lng, lat = point_text.replace("Point(", "").replace(")", "").split(" ")
    lng = float(lng) + random.uniform(-0.015, 0.015)
    lat = float(lat) + random.uniform(-0.015, 0.015)

    return {"longitude": lng, "latitude": lat, "limit": 5, "radius": 1000}


def get_postcode(x):
    if x:
        if x["result"]:

            return [r["postcode"] for r in x["result"]]
    else:
        return None


def postcode_lookup_from_cleaned_person_data(df, api_group_size=100):

    cols = ["human", "birth_coordinates", "residence_coordinates"]
    df["points"] = list(df[cols].itertuples(index=False, name=None))

    cols = ["human", "points", "birth_coordinates", "residence_coordinates"]

    df = df[cols].copy()
    df["point_array"] = df["points"].map(points_to_array)

    df_exploded = df[["point_array"]].explode("point_array")
    df_exploded = df_exploded[["point_array"]].dropna()

    df_exploded["group"] = np.floor(np.arange(len(df_exploded)) / api_group_size)

    point_lists = df_exploded.groupby("group")["point_array"].apply(list).reset_index()

    point_lists["geo_array"] = point_lists["point_array"].apply(
        map_lat_lng, perturb=False
    )
    exploded = point_lists["geo_array"].explode("geo_array")
    df_results_1 = pd.DataFrame(list(exploded))
    df_results_1["postcode"] = df_results_1["pc_response"].apply(get_postcode)

    df_results_1 = df_results_1[["point", "person", "postcode"]]

    point_lists["geo_array"] = point_lists["point_array"].apply(
        map_lat_lng, perturb=True
    )
    exploded = point_lists["geo_array"].explode("geo_array")
    df_results_2 = pd.DataFrame(list(exploded))
    df_results_2["postcode"] = df_results_2["pc_response"].apply(get_postcode)
    df_results_2 = df_results_2[["point", "person", "postcode"]]

    final_results = df_results_1.merge(
        df_results_2,
        left_on=["person", "point"],
        right_on=["person", "point"],
        how="left",
        suffixes=("_orig", "_pert"),
    )

    f1 = final_results["postcode_orig"].isnull()
    f2 = final_results["postcode_orig"].isnull()
    return final_results[~(f1 & f2)]
