import random
import numpy as np
from datetime import timedelta

from corrupt.geco_corrupt import (
    CorruptValueNumpad,
    position_mod_uniform,
)


def date_gen_uncorrupted_record(formatted_master_record, colname, corrupted_record={}):
    corrupted_record["dob"] = str(formatted_master_record[colname])
    return corrupted_record


def date_corrupt_typo(formatted_master_record, colname, corrupted_record={}):

    if formatted_master_record[colname] is None:
        corrupted_record[colname] = None
        return corrupted_record

    dob = str(formatted_master_record[colname])
    numpad_corruptor = CorruptValueNumpad(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )

    dob_ex_year = dob[2:]
    corrupted_dob_ex_year = numpad_corruptor.corrupt_value(dob_ex_year)
    corrupted_record[colname] = dob[:2] + corrupted_dob_ex_year
    if dob != corrupted_record[colname]:
        corrupted_record["num_dob_corruptions"] += 1
    return corrupted_record


def date_corrupt_timedelta(formatted_master_record, colname, corrupted_record={}):

    if formatted_master_record[colname] is None:
        corrupted_record[colname] = None
        return corrupted_record

    dob = formatted_master_record[colname]

    choice = np.random.choice(["small", "medium", "large"], p=[0.7, 0.2, 0.1])

    if choice == "small":
        delta = timedelta(days=random.randint(-5, 5))
    elif choice == "medium":
        delta = timedelta(days=random.randint(-61, 61))
    elif choice == "large":
        delta = timedelta(days=random.randint(1000, 1000))

    corrupted_record["num_dob_corruptions"] += 1

    dob = dob + delta
    corrupted_record[colname] = str(dob)
    return corrupted_record
