# %%
import numpy as np
import random
from corrupt.geco_corrupt import (
    position_mod_uniform,
    CorruptValueQuerty,
)
import math
from scrape_wikidata.postcodes import Api
from scrape_wikidata.cleaning_fns import get_postcode_from_api_results

# TODO:
# - NEARBY LOCATIONS SHOULD BE LIST OF LISTS, NOT FLATTENED LIST


def haversine(lat1, lng1, lat2, lng2):

    # convert decimal degrees to radians
    lng1, lat1, lng2, lat2 = map(math.radians, [lng1, lat1, lng2, lat2])

    # haversine formula
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r


def create_random_point(lat, lng, distance_km):

    radians = random.uniform(0, 2 * math.pi)

    dx = math.sin(radians) * distance_km
    dy = math.cos(radians) * distance_km

    r_earth = 6371  # Radius of earth in kilometers.

    new_lat = lat + (dy / r_earth) * (180 / math.pi)
    new_lng = lng + (dx / r_earth) * (180 / math.pi) / math.cos(lat * math.pi / 180)

    return (new_lat, new_lng)


def get_random_loc_at_distance(loc, distance_km):
    lat = loc["lat"]
    lng = loc["lng"]
    new_lat, new_lng = create_random_point(lat, lng, distance_km)

    api = Api()
    point = {"latitude": new_lat, "longitude": new_lng, "limit": 5, "radius": 1000}

    payload = {
        "geolocations": [
            point,
        ]
    }

    # print("making postcodes.io bulk API request")
    response = api.get_bulk_reverse_geocode(payload)

    response_array = response["result"]
    if len(response_array) > 0:
        return [get_postcode_from_api_results(r) for r in response_array]
    else:
        return None


def get_location_preference_order(master_record, preference_order):
    for key in preference_order:
        if master_record[key] is not None:
            return master_record[key]


def get_all_nonrandom_locs(master_record):
    locs = []
    keys = [
        "parent_birthplace_loc",
        "parent_random_loc",
        "residence_nearby_locs",
        "birthplace_nearby_locs",
    ]
    for k in keys:
        if master_record[k] is not None:
            locs.extend(master_record[k])
    return locs


def get_simulated_house_moves(loc):

    # Get three alternative locations, weighted towards nearby
    # import numpy as np
    # data = list(np.random.pareto(5, 2) * 100)
    # data = pd.DataFrame({'d': data})
    # alt.Chart(data).mark_bar().encode(
    #     alt.X("d:Q",  bin=alt.Bin(extent=[0, 100], step=1)),
    #     y='count()',
    # )

    distances = list(np.random.pareto(5, 3) * 100)

    choices = []
    for d in distances:
        alt_loc = get_random_loc_at_distance(loc, d)
        if alt_loc is not None:
            choices.append(alt_loc[0])
    return choices


def clean_unparished(x):
    if x is not None:
        return x.replace(", unparished area", "")
    else:
        return x


def location_master_record(master_record):

    # This functions:
    # (1).  Chooses a 'master' location for the person from the available information in the master record
    # (2).  Chooses some alternative nearby (within a short distances or so ) postcodes which can act as 'typos'
    # (3).  Chooses some alternative locations further away that can simulate moving house

    # We would prefer not to always use the birth place as the person's 'location', i.e. where they're simulated to 'currently live'.

    pref_order = [
        "parent_birthplace_loc",
        "parent_random_loc",
        "residence_nearby_locs",
        "birthplace_nearby_locs",
        "random_nearby_locs",
    ]
    primary_loc = get_location_preference_order(master_record, pref_order)[0]
    master_record["_primary_loc"] = primary_loc

    # Get alternative locations
    # If we have genuine alternatives, use them.  Otherwise generate some based on the primary loc

    all_locs = get_all_nonrandom_locs(master_record)

    if len(all_locs) < 3:
        loc = primary_loc[0]
        additional_locs = get_simulated_house_moves(loc)

        all_locs.extend(additional_locs)
    master_record["_all_locs"] = all_locs

    return master_record


def birth_place_master_record(master_record):
    if master_record["birth_place"] is not None:
        chosen_birth_place_options = []
        chosen_birth_place_options.append(master_record["birth_place"])

        locs = master_record["birthplace_nearby_locs"]
        if locs is not None:
            loc = locs[0][0]
            choices = [loc["district"], loc["ward"], clean_unparished(loc["parish"])]
            choices = [c for c in choices if c is not None]
            chosen_birth_place_options.extend(choices)
        else:
            # If we have no location, but we do have birth place
            chosen_birth_place_options.append(master_record["birth_place"])
            chosen_birth_place_options.append(master_record["birth_place"])
            chosen_birth_place_options.append(master_record["birth_country"])

    else:
        # If we have no real birth place, get birth place from random location
        chosen_birth_place_options = []
        pref_order = [
            "parent_birthplace_loc",
            "parent_random_loc",
            "residence_nearby_locs",
            "birthplace_nearby_locs",
            "random_nearby_locs",
        ]
        loc = get_location_preference_order(master_record, pref_order)[0][0]

        chosen_birth_place_options = [
            loc["district"],
            loc["ward"],
            clean_unparished(loc["parish"]),
        ]
        chosen_birth_place_options = [
            c for c in chosen_birth_place_options if c is not None
        ]
    master_record["_chosen_birth_place"] = chosen_birth_place_options[0]
    master_record["_chosen_birth_place_options"] = chosen_birth_place_options

    return master_record


# # CORRUPTION FUNCTIONS.  We want 3:

# # 1. Corrupt by choosing an alternative location (simulation of moving house)
# # 2. Corrupt by choosing a valid postcode very close by
# # 3. Corrupt by making a typo in the postcode

# # def birth_place_master_record2(master_record):


# def location_master_record(master_record):

#     # Choose location for master record
#     trial_fns = [
#         get_parent_location_if_exists,
#         get_first_location_if_exists,
#         get_predetermined_random_location,
#     ]

#     loc = None
#     for fn in trial_fns:
#         if loc is None:
#             loc = fn(master_record)
#     master_record["_chosen_location"] = loc

#     # Get two alternative locations, weighted towards nearby
#     # import numpy as np
#     # data = list(np.random.pareto(5, 2) * 100)
#     # data = pd.DataFrame({'d': data})
#     # alt.Chart(data).mark_bar().encode(
#     #     alt.X("d:Q",  bin=alt.Bin(extent=[0, 100], step=1)),
#     #     y='count()',
#     # )

#     distances = list(np.random.pareto(5, 3) * 100)

#     choices = []
#     for d in distances:
#         alt_loc = get_random_loc_at_distance(loc, d)
#         if alt_loc is not None:
#             choices.append(alt_loc[0])

#     master_record["_alt_loc_choices"] = choices

#     return master_record


# def get_parent_location_if_exists(master_record):
#     cols = ["parent_fake_loc", "parent_random_loc"]
#     for col in cols:
#         if master_record[col] is not None:
#             return master_record[col]
#     return None


# def get_alternative_location_if_exists(master_record):
#     if master_record["nearby_postcodes"] is None:
#         return None

#     num_different_locations = len(master_record["nearby_postcodes"])
#     if num_different_locations > 1:
#         nearby_postcodes = np.random.choice(master_record["nearby_postcodes"])
#         return nearby_postcodes[0]
#     else:
#         return None


# def get_nearby_location_if_exists(master_record):
#     if master_record["nearby_postcodes"] is None:
#         return None
#     return np.random.choice(master_record["nearby_postcodes"][0])


# def get_predetermined_random_location(master_record):
#     return master_record["random_pc"]


# def get_first_location_if_exists(master_record):
#     if master_record["nearby_postcodes"] is None:
#         return None
#     return master_record["nearby_postcodes"][0][0]


# def get_random_birth_place(master_record, corrupted_record={}):

#     choices = master_record["_chosen_birth_place_options"]

#     choice = np.random.choice(choices)

#     corrupted_record["birth_place"] = choice
#     return corrupted_record


# def location_exact_match(master_record, corrupted_record={}):

#     loc = master_record["_chosen_location"]
#     corrupted_record["postcode"] = loc["postcode"]
#     corrupted_record["lat"] = loc["lat"]
#     corrupted_record["lng"] = loc["lng"]

#     return corrupted_record


# def birth_place_exact_match(master_record, corrupted_record={}):
#     corrupted_record["birth_place"] = master_record["_chosen_birth_place"]
#     return corrupted_record


# def corrupt_birth_place(master_record, corrupted_record={}):

#     corrupted_record["num_birth_place_corruptions"] = 0

#     corrupted_record = get_random_birth_place(master_record, corrupted_record)

#     if master_record["_chosen_birth_place"] != corrupted_record["birth_place"]:
#         corrupted_record["num_corruptions"] += 1
#         corrupted_record["num_birth_place_corruptions"] += 1

#     return corrupted_record


# def get_alternative_random_location(master_record):

#     alt_locs = master_record["_alt_loc_choices"]
#     alt_locs.append(master_record["_chosen_location"])

#     choice = np.random.choice(alt_locs)

#     return choice


# def corrupt_location(master_record, corrupted_record={}):

#     corrupted_record["num_location_corruptions"] = 0
#     trial_fns = [
#         get_parent_location_if_exists,
#         get_alternative_location_if_exists,
#         get_alternative_random_location,
#     ]

#     loc = None
#     for fn in trial_fns:
#         if loc is None:
#             loc = fn(master_record)

#     corrupted_record["postcode"] = loc["postcode"]
#     corrupted_record["lat"] = loc["lat"]
#     corrupted_record["lng"] = loc["lng"]

#     if master_record["_chosen_location"]["postcode"] != corrupted_record["postcode"]:
#         corrupted_record["num_corruptions"] += 1
#         corrupted_record["num_location_corruptions"] += 1

#     return corrupted_record


# def location_null(master_record, null_prob, corrupted_record={}):

#     if random.uniform(0, 1) < null_prob:
#         corrupted_record["postcode"] = None
#         corrupted_record["lat"] = None
#         corrupted_record["lng"] = None
#     return corrupted_record
