import random
import numpy as np
from datetime import timedelta

from corrupt.geco_corrupt import (
    CorruptValueNumpad,
    position_mod_uniform,
)


def date_gen_uncorrupted_record(formatted_master_record, colname, record_to_modify={}):
    record_to_modify["dob"] = str(formatted_master_record[colname])
    return record_to_modify


def date_corrupt_typo(formatted_master_record, colname, record_to_modify={}):

    if formatted_master_record[colname] is None:
        record_to_modify[colname] = None
        return record_to_modify

    dob = str(formatted_master_record[colname])
    numpad_corruptor = CorruptValueNumpad(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )

    dob_ex_year = dob[2:]
    corrupted_dob_ex_year = numpad_corruptor.corrupt_value(dob_ex_year)
    record_to_modify[colname] = dob[:2] + corrupted_dob_ex_year
    if dob != record_to_modify[colname]:
        record_to_modify["num_dob_corruptions"] += 1
    return record_to_modify


def date_corrupt_timedelta(formatted_master_record, colname, record_to_modify={}):

    if formatted_master_record[colname] is None:
        record_to_modify[colname] = None
        return record_to_modify

    dob = formatted_master_record[colname]

    choice = np.random.choice(["small", "medium", "large"], p=[0.7, 0.2, 0.1])

    if choice == "small":
        delta = timedelta(days=random.randint(-5, 5))
    elif choice == "medium":
        delta = timedelta(days=random.randint(-61, 61))
    elif choice == "large":
        delta = timedelta(days=random.randint(1000, 1000))

    record_to_modify["num_dob_corruptions"] += 1

    dob = dob + delta
    record_to_modify[colname] = str(dob)
    return record_to_modify
