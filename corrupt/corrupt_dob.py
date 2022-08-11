import random
import numpy as np
from datetime import timedelta

from corrupt.geco_corrupt import (
    CorruptValueNumpad,
    position_mod_uniform,
)


def dob_format_master_record(master_input_record):
    if master_input_record["dob"][0] is None:
        master_input_record["dob"] = None
    else:
        master_input_record["dob"] = master_input_record["dob"][0]
    return master_input_record


def dob_gen_uncorrupted_record(formatted_master_record, uncorrupted_record={}):
    uncorrupted_record["dob"] = formatted_master_record["dob"]
    return uncorrupted_record


def dob_corrupt_typo(formatted_master_record, corrupted_record={}):

    if formatted_master_record["dob"] is None:
        corrupted_record["dob"] = None
        return corrupted_record

    formatted_master_record["dob"] = str(formatted_master_record["dob"])
    numpad_corruptor = CorruptValueNumpad(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )
    dob = formatted_master_record["dob"]
    dob_ex_year = formatted_master_record["dob"][2:]
    corrupted_dob_ex_year = numpad_corruptor.corrupt_value(dob_ex_year)
    corrupted_record["dob"] = dob[:2] + corrupted_dob_ex_year
    if dob != corrupted_record["dob"]:
        corrupted_record["num_dob_corruptions"] += 1
    return corrupted_record


def dob_corrupt_timedelta(formatted_master_record, corrupted_record={}):

    if formatted_master_record["dob"] is None:
        corrupted_record["dob"] = None
        return corrupted_record

    dob = formatted_master_record["dob"]

    choice = np.random.choice(["small", "medium", "large"], p=[0.7, 0.2, 0.1])

    if choice == "small":
        delta = timedelta(days=random.randint(-5, 5))
    elif choice == "medium":
        delta = timedelta(days=random.randint(-61, 61))
    elif choice == "large":
        delta = timedelta(days=random.randint(1000, 1000))

    corrupted_record["num_dob_corruptions"] += 1

    dob = dob + delta
    corrupted_record["dob"] = str(dob)
    return corrupted_record
