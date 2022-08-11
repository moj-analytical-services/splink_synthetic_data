from corrupt.geco_corrupt import (
    position_mod_uniform,
    CorruptValueQuerty,
)
import numpy as np
import random


def full_name_gen_uncorrupted_record(master_record, uncorrupted_record={}):
    uncorrupted_record["full_name"] = master_record["humanLabel"][0]
    return uncorrupted_record


def full_name_corrupt(formatted_master_record, corrupted_record={}):
    options = formatted_master_record["full_name_arr"]
    if options is None:
        corrupted_record["full_name"] = None
    elif len(options) == 1:
        corrupted_record["full_name"] = options[0]
    else:
        corrupted_record["full_name"] = np.random.choice(options)
        corrupted_record["num_name_corruptions"] += 1
    return corrupted_record


def full_name_null(master_record, null_prob, corrupted_record={}):

    corrupted_record["full_name"] = None
    return corrupted_record
