import numpy as np


def occupation_format_master_record(master_input_record):
    if not master_input_record["occupationLabel"]:
        master_input_record["_list_occupations"] = None
    else:
        master_input_record["_list_occupations"] = master_input_record[
            "occupationLabel"
        ]
    return master_input_record


def occupation_gen_uncorrupted_record(formatted_master_record, record_to_modify={}):
    if formatted_master_record["_list_occupations"] is None:
        record_to_modify["occupation"] = None
    else:
        record_to_modify["occupation"] = ", ".join(
            formatted_master_record["_list_occupations"]
        )
    return record_to_modify


def occupation_corrupt(formatted_master_record, record_to_modify):
    options = formatted_master_record["_list_occupations"]
    if options is None:
        record_to_modify["occupation"] = None
    elif len(options) == 1:
        record_to_modify["occupation"] = options[0]
    else:
        record_to_modify["occupation"] = np.random.choice(list(options))

    return record_to_modify
