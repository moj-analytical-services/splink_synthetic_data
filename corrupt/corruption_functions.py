from corrupt.geco_corrupt import (
    CorruptValueNumpad,
    position_mod_uniform,
)
import numpy as np


def country_citizenship_exact_match(master_record, corrupted_record={}):

    if master_record["country_citizenship"] is None:
        corrupted_record["citizenship"] = None
        return corrupted_record

    corrupted_record["citizenship"] = master_record["country_citizenship"][0]

    return corrupted_record


def country_citizenship_random(master_record, corrupted_record={}):

    if master_record["country_citizenship"] is None:
        corrupted_record["citizenship"] = None
        return corrupted_record

    corrupted_record["citizenship"] = np.random.choice(
        master_record["country_citizenship"]
    )

    return corrupted_record


def dob_exact_match(master_record, corrupted_record={}):
    corrupted_record["dob"] = master_record["dob"]
    return corrupted_record


# TODO: corruption based on distance in time


def dob_corrupt_typo(master_record, corrupted_record={}):

    if master_record["dob"] is None:
        corrupted_record["dob"] = None
        return corrupted_record

    numpad_corruptor = CorruptValueNumpad(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )
    dob = master_record["dob"]
    corrupted_record["dob"] = numpad_corruptor.corrupt_value(dob)
    corrupted_record["dob"] = dob[:2] + corrupted_record["dob"][2:]
    return corrupted_record


def birth_place_exact_match(master_record, corrupted_record={}):
    corrupted_record["birth_place"] = master_record["birth_place"]
    return corrupted_record


def residence_place_exact_match(master_record, corrupted_record={}):
    corrupted_record["residence_place"] = master_record["residence_place"]
    return corrupted_record


def gender_exact_match(master_record, corrupted_record={}):
    corrupted_record["gender"] = master_record["gender"]
    return corrupted_record
