import random
import numpy as np
from datetime import timedelta
from datetime import datetime

from corrupt.geco_corrupt import (
    CorruptValueNumpad,
    position_mod_uniform,
)


def date_gen_uncorrupted_record(
    formatted_master_record, input_colname, output_colname, record_to_modify={}
):
    record_to_modify[output_colname] = str(formatted_master_record[input_colname])
    return record_to_modify


def date_corrupt_typo(
    formatted_master_record, input_colname, output_colname, record_to_modify={}
):

    if not formatted_master_record[input_colname]:
        record_to_modify[output_colname] = None
        return record_to_modify

    input_value_as_str = str(formatted_master_record[input_colname])
    numpad_corruptor = CorruptValueNumpad(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )

    dob_ex_year = input_value_as_str[2:]
    corrupted_dob_ex_year = numpad_corruptor.corrupt_value(dob_ex_year)
    record_to_modify[output_colname] = input_value_as_str[:2] + corrupted_dob_ex_year

    return record_to_modify


def date_corrupt_timedelta(
    formatted_master_record, record_to_modify, input_colname, output_colname
):

    if not record_to_modify[input_colname]:
        return record_to_modify

    input_value = record_to_modify[input_colname]
    input_value = datetime.fromisoformat(input_value)

    choice = np.random.choice(["small", "medium", "large"], p=[0.7, 0.2, 0.1])

    if choice == "small":
        delta = timedelta(days=random.randint(-5, 5))
    elif choice == "medium":
        delta = timedelta(days=random.randint(-61, 61))
    elif choice == "large":
        delta = timedelta(days=random.randint(1000, 1000))

    input_value = input_value + delta
    record_to_modify[output_colname] = str(input_value.date())

    return record_to_modify


def date_corrupt_jan_first(
    formatted_master_record, record_to_modify, input_colname, output_colname
):

    if not record_to_modify[input_colname]:
        return record_to_modify

    input_value = record_to_modify[input_colname]
    record_to_modify[output_colname] = str(input_value)[:4] + "-01-01"

    return record_to_modify
