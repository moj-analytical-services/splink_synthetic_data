from corrupt.geco_corrupt import CorruptValueNumpad, position_mod_uniform
import numpy as np

## Citizenship
def country_citizenship_exact_match(master_record, corrupted_record={}):

    if master_record["country_citizenship"] is None:
        corrupted_record["citizenship"] = None
        return corrupted_record

    corrupted_record["citizenship"] = master_record["country_citizenship"][0]
    if corrupted_record["citizenship"] in (
        "United Kingdom",
        "United Kingdom of Great Britain and Ireland",
    ):
        corrupted_record["citizenship"] = "UK"

    return corrupted_record


def country_citizenship_random(master_record, corrupted_record={}):

    if master_record["country_citizenship"] is None:
        corrupted_record["citizenship"] = None
        return corrupted_record

    corrupted_record["citizenship"] = np.random.choice(
        master_record["country_citizenship"]
    )
    if corrupted_record["citizenship"] in (
        "United Kingdom",
        "United Kingdom of Great Britain and Ireland",
    ):
        corrupted_record["citizenship"] = "UK"
    return corrupted_record


## DOB
def dob_exact_match(master_record, corrupted_record={}):
    corrupted_record["dob"] = master_record["dob"]
    return corrupted_record


def dob_corrupt_typo(master_record, corrupted_record={}):

    if master_record["dob"] is None:
        corrupted_record["dob"] = None
        return corrupted_record

    numpad_corruptor = CorruptValueNumpad(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )
    dob = master_record["dob"]
    corrupted_record["dob"] = numpad_corruptor.corrupt_value(dob)
    return corrupted_record


## Birth place
def birth_place_exact_match(master_record, corrupted_record={}):
    corrupted_record["birth_place"] = master_record["birth_place"]
    return corrupted_record


## Residence place
def residence_place_exact_match(master_record, corrupted_record={}):
    corrupted_record["residence_place"] = master_record["residence_place"]
    return corrupted_record


## Name


def full_name_exact_match(master_record, corrupted_record={}):
    corrupted_record["full_name"] = master_record["humanlabel"]
    return corrupted_record


def full_name_use_alt_label_if_exists(master_record, corrupted_record={}):
    if master_record["humanaltlabel"] is None:
        corrupted_record = full_name_use_alt_given_names(
            master_record, corrupted_record
        )
        return corrupted_record

    corrupted_record["full_name"] = np.random.choice(master_record["humanaltlabel"])
    return corrupted_record


def get_alt_name_lookup(master_record):
    lkup = {}
    for i in [1, 2, 3]:
        if master_record[f"given_name_{i}"] is not None:
            if master_record[f"alt_given_name_{i}"] is not None:
                lkup[master_record[f"given_name_{i}"]] = master_record[
                    f"alt_given_name_{i}"
                ]

    for i in [1, 2]:
        if master_record[f"family_name_{i}"] is not None:
            if master_record[f"alt_family_name_{i}"] is not None:
                lkup[master_record[f"family_name_{i}"]] = master_record[
                    f"alt_family_name_{i}"
                ]
    return lkup


def full_name_use_alt_given_names(
    master_record, corrupted_record={}, prob_threshold=0.0
):

    master_name = master_record["humanlabel"]
    master_name_parts = master_name.split(" ")
    alt_name_lookup = get_alt_name_lookup(master_record)

    new_name = []
    not_in_master_name = []
    for p in master_name_parts:
        this_name = p
        if p in alt_name_lookup:
            if np.random.uniform(0, 1) > prob_threshold:
                names_weights = alt_name_lookup[p]
                names = [i["alt_name"] for i in names_weights]
                weights = [i["count"] for i in names_weights]
                weights = [i / sum(weights) for i in weights]
                this_name = np.random.choice(names, p=weights)

        new_name.append(this_name)

    # There may be given names which are not in the master name.
    for k in alt_name_lookup.keys():
        if k not in master_name_parts:
            if np.random.uniform(0, 1) > prob_threshold:
                names_weights = alt_name_lookup[k]
                names = [i["alt_name"] for i in names_weights]
                weights = [i["count"] for i in names_weights]
                weights = [i / sum(weights) for i in weights]
                this_name = np.random.choice(names, p=weights)
                new_name.insert(-1, this_name)

    corrupted_record["full_name"] = " ".join(new_name)
    return corrupted_record


## Postcode/Location


def postcode_lat_lng_exact_match(master_record, corrupted_record={}):
    corrupted_record["fake_postcode"] = master_record["fake_postcode"]
    corrupted_record["fake_lat"] = master_record["fake_lat"]
    corrupted_record["fake_lng"] = master_record["fake_lng"]
    return corrupted_record


def postcode_lat_lng_alternative(master_record, corrupted_record={}):
    if master_record["fake_postcode"]:
        num_different_locations = len(master_record["nearby_postcodes"])
        location_index = np.random.choice(range(num_different_locations))
        postcode_options = master_record["nearby_postcodes"][location_index]
        num_postcode_options = len(postcode_options)
        pc_index = np.random.choice(range(1, num_postcode_options))
        try:
            postcode = postcode_options[pc_index]
        except IndexError:
            postcode = postcode_options[0]
        corrupted_record["fake_postcode"] = postcode["postcode"]
        corrupted_record["fake_lat"] = postcode["lat"]
        corrupted_record["fake_lng"] = postcode["lng"]

    else:
        corrupted_record["fake_postcode"] = None
        corrupted_record["fake_lat"] = None
        corrupted_record["fake_lng"] = None

    return corrupted_record
