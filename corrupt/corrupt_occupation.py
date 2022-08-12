import numpy as np


def occupation_format_master_record(master_input_record):
    if master_input_record["occupationLabel"][0] is None:
        master_input_record["_list_occupations"] = None
    else:
        master_input_record["_list_occupations"] = master_input_record[
            "occupationLabel"
        ]
    return master_input_record


def occupation_gen_uncorrupted_record(formatted_master_record, corrupted_record={}):
    if formatted_master_record["_list_occupations"] is None:
        corrupted_record["occupation"] = None
    else:
        corrupted_record["occupation"] = ", ".join(
            formatted_master_record["_list_occupations"]
        )
    return corrupted_record


def occupation_corrupt(formatted_master_record, corrupted_record={}):
    options = formatted_master_record["_list_occupations"]
    if options is None:
        corrupted_record["occupation"] = None
    elif len(options) == 1:
        corrupted_record["occupation"] = options[0]
    else:
        corrupted_record["occupation"] = np.random.choice(list(options))
        corrupted_record["num_occupation_corruptions"] += 1
    return corrupted_record
