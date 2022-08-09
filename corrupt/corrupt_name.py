from corrupt.geco_corrupt import (
    position_mod_uniform,
    CorruptValueQuerty,
)
import numpy as np
import random


def full_name_gen_uncorrupted_record(master_record, uncorrupted_record={}):
    uncorrupted_record["full_name"] = master_record["humanLabel"][0]
    return uncorrupted_record


def get_alt_name_lookup(master_record):
    lkup = {}
    for i in [0, 1, 2]:
        if master_record["given_name"][i] is not None:
            if master_record["alt_given_name"][i] is not None:
                lkup[master_record["given_name"][i]] = master_record["alt_given_name"][
                    i
                ]

    for i in [0, 1]:
        if master_record["family_name"][i] is not None:
            if master_record["alt_family_name"][i] is not None:
                lkup[master_record["family_name"][i]] = master_record[
                    "alt_family_name"
                ][i]
    return lkup


def full_name_corrupt(formatted_master_record, corrupted_record={}):

    # Get list of labelled names and pick one
    list_of_names = []
    list_of_alt_names = []
    list_of_names.extend(formatted_master_record["humanLabel"])
    if formatted_master_record["humanAltLabel"][0] is not None:
        list_of_names.extend(formatted_master_record["humanAltLabel"])
        list_of_alt_names = formatted_master_record["humanAltLabel"]

    # If we have alt names, sometimes return an alt name
    #  using the following probabilities
    len_prob_lookup = {1: 0.25, 2: 0.33, 3: 0.5}

    if len(list_of_names) > 1:
        len_an = len(list_of_alt_names)
        prob = len_prob_lookup.get(len_an, 0.75)

        if random.uniform(0, 1) < prob:
            corrupted_record["full_name"] = np.random.choice(list_of_alt_names)
            return corrupted_record

    corrupted_record["full_name"] = np.random.choice(list_of_names)

    if corrupted_record["full_name"] != formatted_master_record["humanLabel"][0]:
        corrupted_record["num_name_corruptions"] += 2

    # Corrupt the name using alternative forms of given names
    corrupted_record = corrupt_using_said_to_be_same_as(
        formatted_master_record, corrupted_record
    )

    # Corrupt the name using typos
    corrupted_record = corrupt_name_querty(formatted_master_record, corrupted_record)
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

    new_name = [n for n in new_name if n is not None]
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


def full_name_null(master_record, null_prob, corrupted_record={}):

    new_name = corrupted_record["full_name"].split(" ")

    try:
        first = new_name.pop(0)
    except IndexError:
        first = None
    try:
        last = new_name.pop()
    except IndexError:
        last = None

    # Erase middle names with probability 0.5
    new_name = [n for n in new_name if random.uniform(0, 1) < 0.5]

    # Erase first or last name with prob null prob
    if random.uniform(0, 1) < null_prob:
        if random.uniform(0, 1) < 0.5:
            first = None
        else:
            last = None

    new_name = [first] + new_name + [last]

    new_name = [n for n in new_name if n is not None]
    if len(new_name) > 0:
        corrupted_record["full_name"] = " ".join(new_name)
    else:
        corrupted_record["full_name"] = None
    return corrupted_record
