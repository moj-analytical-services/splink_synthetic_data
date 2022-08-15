import numpy as np


def occupation_format_master_record(master_input_record):
    if master_input_record["occupationLabel"][0] is None:
        master_input_record["_list_occupations"] = None
    else:
        master_input_record["_list_occupations"] = master_input_record[
            "occupationLabel"
        ]
    return master_input_record


def occupation_gen_uncorrupted_record(formatted_master_record, input_record={}):
    if formatted_master_record["_list_occupations"] is None:
        input_record["occupation"] = None
    else:
        input_record["occupation"] = ", ".join(
            formatted_master_record["_list_occupations"]
        )
    return input_record


def occupation_corrupt(formatted_master_record, input_record={}):
    options = formatted_master_record["_list_occupations"]
    if options is None:
        input_record["occupation"] = None
    elif len(options) == 1:
        input_record["occupation"] = options[0]
    else:
        input_record["occupation"] = np.random.choice(list(options))
        input_record["num_occupation_corruptions"] += 1
    return input_record
