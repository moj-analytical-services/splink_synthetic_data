import random
import numpy as np
from datetime import timedelta

from corrupt.geco_corrupt import (
    CorruptValueNumpad,
    position_mod_uniform,
)


def dob_format_master_record(master_input_record):
    if not master_input_record["dob"]:
        master_input_record["dob"] = None
    else:
        master_input_record["dob"] = master_input_record["dob"][0]
    return master_input_record


def dob_gen_uncorrupted_record(formatted_master_record, uncorrupted_record={}):
    uncorrupted_record["dob"] = formatted_master_record["dob"]
    return uncorrupted_record
