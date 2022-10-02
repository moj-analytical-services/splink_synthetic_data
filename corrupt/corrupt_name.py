from multiprocessing.sharedctypes import Value
import numpy as np
import functools
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
    full_name = (
        " ".join(master_record["given_nameLabel"])
        + " "
        + " ".join(master_record["family_nameLabel"])
    )
    record_to_modify["full_name"] = full_name.lower()
    return record_to_modify


def full_name_alternative(formatted_master_record, record_to_modify):
    """Choose an alternative full name if one exists"""

    options = formatted_master_record["full_name_arr"]
    if options is None:
        full_name = None
    elif len(options) == 1:
        full_name = options[0]
    else:
        full_name = np.random.choice(options)
    record_to_modify["full_name"] = full_name.lower()

    return record_to_modify


def each_name_alternatives(formatted_master_record, record_to_modify):
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
            try:
                output_names.append(np.random.choice(alt_names, p=weights))
            except ValueError:
                print(weights)

        elif n in family_name_alt_lookup:
            name_dict = family_name_alt_lookup[n]
            alt_names = name_dict["alt_name_arr"]
            weights = name_dict["alt_name_weight_arr"]
            try:
                output_names.append(np.random.choice(alt_names, p=weights))
            except ValueError:
                print(weights)

        else:
            output_names.append(n)

    record_to_modify["full_name"] = " ".join(output_names)

    return record_to_modify


def full_name_typo(formatted_master_record, record_to_modify):

    full_name = record_to_modify["full_name"]

    querty_corruptor = CorruptValueQuerty(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )

    record_to_modify["full_name"] = querty_corruptor.corrupt_value(full_name)

    return record_to_modify


def name_inversion(formatted_master_record, record_to_modify):

    given = formatted_master_record["given_nameLabel"]
    family = formatted_master_record["family_nameLabel"]

    if len(given) > 0 and len(family) > 0:
        full_name = family[0] + " " + given[0]
    record_to_modify["full_name"] = full_name.lower()

    return record_to_modify
