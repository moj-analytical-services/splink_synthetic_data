import numpy as np


def occupation_master_record(master_record):
    options = master_record["occupation_options"]
    if options is not None:
        master_record["_chosen_occupation"] = np.random.choice(list(options))
    else:
        master_record["_chosen_occupation"] = None
    return master_record


def occupation_exact_match(master_record, corrupted_record={}):
    corrupted_record["occupation"] = master_record["_chosen_occupation"]
    return corrupted_record


def occupation_corrupt(master_record, corrupted_record={}):
    options = master_record["occupation_options"]
    if options is not None:
        corrupted_record["occupation"] = np.random.choice(list(options))
    else:
        corrupted_record["occupation"] = None
    return corrupted_record
