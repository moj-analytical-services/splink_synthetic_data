import numpy as np
import functools

import pandas as pd


@functools.lru_cache(maxsize=None)
def get_given_name_alternatives_lookup():
    in_path = "out_data/wikidata/processed/alt_name_lookups/given_name_lookup.parquet"
    df = pd.read_parquet(in_path).set_index("original_name")
    return df.to_dict(orient="index")


@functools.lru_cache(maxsize=None)
def get_family_name_alternatives_lookup():
    in_path = "out_data/wikidata/processed/alt_name_lookups/family_name_lookup.parquet"
    df = pd.read_parquet(in_path).set_index("original_name")
    return df.to_dict(orient="index")


def full_name_gen_uncorrupted_record(master_record, uncorrupted_record={}):
    uncorrupted_record["full_name"] = master_record["humanLabel"][0]
    return uncorrupted_record


def full_name_alternative(formatted_master_record, corrupted_record={}):
    """Choose an alternative full name if one exists"""

    options = formatted_master_record["full_name_arr"]
    if options is None:
        corrupted_record["full_name"] = None
    elif len(options) == 1:
        corrupted_record["full_name"] = options[0]
    else:
        corrupted_record["full_name"] = np.random.choice(options).lower()
        corrupted_record["num_name_corruptions"] += 1
    return corrupted_record


def each_name_alternatives(formatted_master_record, corrupted_record={}):
    """Choose a full name if one exists"""

    options = formatted_master_record["full_name_arr"]

    if options is None:
        corrupted_record["full_name"] = None
        return corrupted_record

    full_name = options[0]

    names = full_name.split(" ")

    given_name_alt_lookup = get_given_name_alternatives_lookup()
    family_name_alt_lookup = get_family_name_alternatives_lookup()

    output_names = []
    for n in names:
        n = n.lower()
        if n in given_name_alt_lookup:
            name_dict = given_name_alt_lookup[n]
            alt_names = name_dict["alt_name_arr"]
            weights = name_dict["alt_name_weight_arr"]
            output_names.append(np.random.choice(alt_names, p=weights))
            corrupted_record["num_name_corruptions"] += 1
        elif n in family_name_alt_lookup:
            name_dict = family_name_alt_lookup[n]
            alt_names = name_dict["alt_name_arr"]
            weights = name_dict["alt_name_weight_arr"]
            output_names.append(np.random.choice(alt_names, p=weights))
            corrupted_record["num_name_corruptions"] += 1
        else:
            output_names.append(n)

    corrupted_record["full_name"] = " ".join(output_names).lower()

    return corrupted_record


def full_name_null(master_record, null_prob, corrupted_record={}):

    corrupted_record["full_name"] = None
    return corrupted_record
