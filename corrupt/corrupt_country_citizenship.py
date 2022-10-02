import numpy as np


def country_citizenship_format_master_record(master_input_record):
    if not master_input_record["country_citizenLabel"]:
        master_input_record["_list_country_citizenship"] = None
    else:
        master_input_record["_list_country_citizenship"] = master_input_record[
            "country_citizenLabel"
        ]
    return master_input_record


def country_citizenship_gen_uncorrupted_record(
    formatted_master_record, record_to_modify={}
):
    if formatted_master_record["_list_country_citizenship"] is None:
        record_to_modify["country_citizenship"] = None
    else:
        record_to_modify["country_citizenship"] = ", ".join(
            formatted_master_record["_list_country_citizenship"]
        )
    return record_to_modify


def country_citizenship_corrupt(formatted_master_record, record_to_modify):
    options = formatted_master_record["_list_country_citizenship"]
    if options is None:
        record_to_modify["country_citizenship"] = None
    elif len(options) == 1:
        record_to_modify["country_citizenship"] = options[0]
    else:
        record_to_modify["country_citizenship"] = np.random.choice(list(options))

    return record_to_modify
