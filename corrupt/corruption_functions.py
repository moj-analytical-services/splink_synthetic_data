from functools import partial
import random


def master_record_no_op(master_record):
    return master_record


def _basic_null_fn_to_partial(master_record, col_name, null_prob, corrupted_record={}):

    if random.uniform(0, 1) < null_prob:
        corrupted_record[col_name] = None

    return corrupted_record


def basic_null_fn(colname):
    return partial(_basic_null_fn_to_partial, col_name=colname)
