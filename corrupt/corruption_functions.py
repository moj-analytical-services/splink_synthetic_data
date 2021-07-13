from corrupt.geco_corrupt import (
    CorruptValueNumpad,
    position_mod_uniform,
    CorruptValueQuerty,
)
import numpy as np
import random

## Citizenship
def country_citizenship_exact_match(master_record, corrupted_record={}):

    if master_record["country_citizenship"] is None:
        corrupted_record["citizenship"] = None
        return corrupted_record

    corrupted_record["citizenship"] = master_record["country_citizenship"][0]

    return corrupted_record


def country_citizenship_random(master_record, corrupted_record={}):

    if master_record["country_citizenship"] is None:
        corrupted_record["citizenship"] = None
        return corrupted_record

    corrupted_record["citizenship"] = np.random.choice(
        master_record["country_citizenship"]
    )

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
    corrupted_record["dob"] = dob[:2] + corrupted_record["dob"][2:]
    return corrupted_record


## Birth place
def birth_place_exact_match(master_record, corrupted_record={}):
    corrupted_record["birth_place"] = master_record["birth_place"]
    return corrupted_record


## Residence place
def residence_place_exact_match(master_record, corrupted_record={}):
    corrupted_record["residence_place"] = master_record["residence_place"]
    return corrupted_record


## Gender
def gender_exact_match(master_record, corrupted_record={}):
    corrupted_record["gender"] = master_record["gender"]
    return corrupted_record


## Name


def full_name_exact_match(master_record, corrupted_record={}):
    corrupted_record["full_name"] = master_record["humanlabel"]
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


# If there's an alt label, pick


def corrupt_full_name(master_record, corrupted_record={}):

    corrupted_record["num_name_corruptions"] = 0

    # Get list of labelled names and pick one
    list_of_names = []
    list_of_names.append(master_record["humanlabel"])
    if master_record["humanaltlabel"] is not None:
        list_of_names.extend(master_record["humanaltlabel"])

    corrupted_record["full_name"] = np.random.choice(list_of_names)

    if corrupted_record["full_name"] != master_record["humanlabel"]:
        corrupted_record["num_name_corruptions"] += 2

    # Corrupt the name using alternative forms of given names
    corrupted_record = corrupt_using_said_to_be_same_as(master_record, corrupted_record)

    # Corrupt the name using typos
    corrupted_record = corrupt_name_querty(master_record, corrupted_record)
    corrupted_record

    corrupted_record["num_corruptions"] += corrupted_record["num_name_corruptions"]
    return corrupted_record


def random_choice_from_names_weights(names_weights, orig_name):
    names = [i["alt_name"] for i in names_weights]
    weights = [i["weight"] for i in names_weights]
    weights = [i / sum(weights) for i in weights]

    names.append(orig_name)

    weights.append(0.5)
    weights = [i / sum(weights) for i in weights]

    this_name = np.random.choice(names, p=weights)
    return this_name


def update_used_names(used_set, names_weights):
    names = [i["alt_name"] for i in names_weights]
    used_set = used_set.union(names)
    return used_set


def corrupt_using_said_to_be_same_as(
    master_record, corrupted_record={}, prob_of_corrupt=0.5
):

    master_name = corrupted_record["full_name"]
    master_name_parts = master_name.split(" ")
    master_name_parts = [p for p in master_name_parts if len(p.replace(".", "")) > 1]
    alt_name_lookup = get_alt_name_lookup(master_record)

    new_name = []
    used_names = set()

    # Use the full name - important because this includes the order of forenames and surname
    for name_part in master_name_parts:

        if name_part in alt_name_lookup:
            names_weights = alt_name_lookup[name_part]
            this_name = random_choice_from_names_weights(names_weights, name_part)
            if this_name != name_part:
                corrupted_record["num_name_corruptions"] += 1
            new_name.append(this_name)
            used_names = update_used_names(used_names, names_weights)
        else:
            new_name.append(name_part)

        used_names = used_names.union([name_part])

    # There may be given names which are not in the master name - if so insert into the middle
    list_of_names_parts = list(alt_name_lookup.keys())
    for name_part in list_of_names_parts:
        if name_part not in used_names:

            if name_part not in master_name_parts:
                names_weights = alt_name_lookup[name_part]
                names_weights = alt_name_lookup[name_part]
                this_name = random_choice_from_names_weights(names_weights, name_part)
                if this_name not in used_names:
                    new_name.insert(-1, this_name)
                    corrupted_record["num_name_corruptions"] += 1

    first = new_name.pop(0)
    last = new_name.pop()
    new_name = [n for n in new_name if random.uniform(0, 1) > 0.5]

    new_name = [first] + new_name + [last]
    corrupted_record["full_name"] = " ".join(new_name)
    return corrupted_record


def corrupt_name_querty(master_record, corrupted_record={}):
    querty_corruptor = CorruptValueQuerty(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )

    if corrupted_record["num_name_corruptions"] == 0:

        corrupted_record["full_name"] = querty_corruptor.corrupt_value(
            corrupted_record["full_name"]
        )
    corrupted_record["num_name_corruptions"] += 1
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
