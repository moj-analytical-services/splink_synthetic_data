import numpy as np
import random
from corrupt.geco_corrupt import (
    position_mod_uniform,
    CorruptValueQuerty,
)


# Location corruption workflow:

# 1. Choose 'current location' from the list of locations in master record
#    - If we know the location of the persons's parent use their location (happens v. rarely)
#    - If we know (from wikidata) the person's real location (e.g. birht place, residence), use the first one observed
#    - Otherwise, if we don't know their 'real' location, using a random location.
#    - Write this location to master record['_loc'] so 'corruptions' know which was selected.
#    This happens as part of location_exact_match

# To corrupt
# 2. Corrupt location
#    - Always use the parent's location if exists.  This deliverately creates 'possibly problematic' clusters of records
#    - Otherwise, if the person has more than one 'real' location,


def location_master_record(master_record):
    # Some master records have a birth place.
    # If we have a real birth place, use it - adding variants on the name of the birthplace
    if master_record["birth_place"] is not None:
        chosen_birth_place_options = []
        chosen_birth_place_options.append(master_record["birth_place"])

        loc = get_first_location_if_exists(master_record)
        if loc is not None:
            choices = [loc["district"], loc["ward"], clean_unparished(loc["parish"])]
            choices = [c for c in choices if c is not None]
            chosen_birth_place_options.extend(choices)
        else:
            # Weight options place rather than country
            chosen_birth_place_options.append(master_record["birth_place"])
            chosen_birth_place_options.append(master_record["birth_place"])
            chosen_birth_place_options.append(master_record["birth_country"])
    else:
        # If we have no real birth place, get birth place from random location
        chosen_birth_place_options = []
        loc = get_first_location_if_exists(master_record)
        if loc is None:
            loc = get_predetermined_random_location(master_record)
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

    # Choose location for master record
    trial_fns = [
        get_parent_child_location_if_exists,
        get_first_location_if_exists,
        get_predetermined_random_location,
    ]

    loc = None
    for fn in trial_fns:
        if loc is None:
            loc = fn(master_record)
    master_record["_chosen_location"] = loc
    return master_record


def get_parent_child_location_if_exists(master_record):
    cols = ["parent_fake_pc", "parent_random_pc"]
    for col in cols:
        if master_record[col] is not None:
            return master_record[col]
    return None


def get_alternative_location_if_exists(master_record):
    if master_record["nearby_postcodes"] is None:
        return None

    num_different_locations = len(master_record["nearby_postcodes"])
    if num_different_locations > 1:
        nearby_postcodes = np.random.choice(master_record["nearby_postcodes"])
        return nearby_postcodes[0]
    else:
        return None


def get_nearby_location_if_exists(master_record):
    if master_record["nearby_postcodes"] is None:
        return None
    return np.random.choice(master_record["nearby_postcodes"][0])


def get_predetermined_random_location(master_record):
    return master_record["random_pc"]


def get_first_location_if_exists(master_record):
    if master_record["nearby_postcodes"] is None:
        return None
    return master_record["nearby_postcodes"][0][0]


def clean_unparished(x):
    if x is not None:
        return x.replace(", unparished area", "")
    else:
        return x


def get_random_birth_place(master_record, corrupted_record={}):

    choices = master_record["_chosen_birth_place_options"]
    choice = np.random.choice(choices)
    corrupted_record["birth_place"] = choice
    return corrupted_record


def location_exact_match(master_record, corrupted_record={}):

    loc = master_record["_chosen_location"]
    corrupted_record["postcode"] = loc["postcode"]
    corrupted_record["lat"] = loc["lat"]
    corrupted_record["lng"] = loc["lng"]

    corrupted_record["birth_place"] = master_record["_chosen_birth_place"]

    return corrupted_record


def corrupt_location(master_record, corrupted_record={}):

    corrupted_record["num_location_corruptions"] = 0
    trial_fns = [
        get_parent_child_location_if_exists,
        get_alternative_location_if_exists,
        get_nearby_location_if_exists,
    ]

    loc = None
    for fn in trial_fns:
        if loc is None:
            loc = fn(master_record)

    # If loc is still none, then the only location
    # we have a single, preset random location
    if loc is None:
        loc = get_predetermined_random_location(master_record)
        querty_corruptor = CorruptValueQuerty(
            position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
        )

        pc = querty_corruptor.corrupt_value(loc["postcode"])
        corrupted_record["postcode"] = pc

        corrupted_record["lat"] = None
        corrupted_record["lng"] = None
    else:
        corrupted_record["postcode"] = loc["postcode"]
        corrupted_record["lat"] = loc["lat"]
        corrupted_record["lng"] = loc["lng"]

    corrupted_record = get_random_birth_place(master_record, corrupted_record)

    if master_record["_chosen_location"]["postcode"] != corrupted_record["postcode"]:
        corrupted_record["num_corruptions"] += 1
        corrupted_record["num_location_corruptions"] += 1

    if master_record["_chosen_birth_place"] != corrupted_record["birth_place"]:
        corrupted_record["num_corruptions"] += 1
        corrupted_record["num_location_corruptions"] += 1

    return corrupted_record


def location_null(master_record, null_prob, corrupted_record={}):

    if random.uniform(0, 1) < null_prob:
        corrupted_record["birth_place"] = None

    if random.uniform(0, 1) < null_prob:
        corrupted_record["postcode"] = None
    return corrupted_record
