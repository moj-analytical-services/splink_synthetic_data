import numpy as np
import functools
import random
import pandas as pd

from corrupt.geco_corrupt import CorruptValueQuerty, position_mod_uniform


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


def full_name_gen_uncorrupted_record(master_record, record_to_modify={}):
    record_to_modify["full_name"] = master_record["humanLabel"][0]
    return record_to_modify


def full_name_alternative(formatted_master_record, record_to_modify={}):
    """Choose an alternative full name if one exists"""

    options = formatted_master_record["full_name_arr"]
    if options is None:
        record_to_modify["full_name"] = None
    elif len(options) == 1:
        record_to_modify["full_name"] = options[0]
    else:
        record_to_modify["full_name"] = np.random.choice(options).lower()
    return record_to_modify


def each_name_alternatives(formatted_master_record, record_to_modify={}):
    """Choose a full name if one exists"""

    options = formatted_master_record["full_name_arr"]

    if options is None:
        record_to_modify["full_name"] = None
        return record_to_modify

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

        elif n in family_name_alt_lookup:
            name_dict = family_name_alt_lookup[n]
            alt_names = name_dict["alt_name_arr"]
            weights = name_dict["alt_name_weight_arr"]
            output_names.append(np.random.choice(alt_names, p=weights))

        else:
            output_names.append(n)

    record_to_modify["full_name"] = " ".join(output_names).lower()

    return record_to_modify


def full_name_typo(formatted_master_record, record_to_modify={}):

    options = formatted_master_record["full_name_arr"]

    if options is None:
        record_to_modify["full_name"] = None
        return record_to_modify

    full_name = options[0]

    querty_corruptor = CorruptValueQuerty(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )

    record_to_modify["full_name"] = querty_corruptor.corrupt_value(full_name)

    return record_to_modify


def full_name_null(formatted_master_record, record_to_modify={}):

    new_name = formatted_master_record["full_name_arr"][0].split(" ")

    try:
        first = new_name.pop(0)
    except IndexError:
        first = None
    try:
        last = new_name.pop()
    except IndexError:
        last = None

    # Erase middle names with probability 0.5
    new_name = [n for n in new_name if random.uniform(0, 1) > 0.5]

    # Erase first or last name with prob null prob

    if random.uniform(0, 1) > 1 / 2:
        first = None
    if random.uniform(0, 1) > 1 / 2:
        last = None

    new_name = [first] + new_name + [last]

    new_name = [n for n in new_name if n is not None]
    if len(new_name) > 0:
        record_to_modify["full_name"] = " ".join(new_name)
    else:
        record_to_modify["full_name"] = None
    return record_to_modify
