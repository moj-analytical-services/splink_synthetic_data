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


def reweight(corruption_num, total_num_corruptions, original_weights, start_exact_proportion, end_exact_proportion):

    weights_except_exact = original_weights[:-1]

    exact_weight



    [0,1] is exact match
    [1,0 is exact non match

    [0.5, 0.5]

    [0.01, 0.99]