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


def scale_linear(domain, range):

    domain_span = domain[1] - domain[0]
    range_span = range[1] - range[0]

    def scale_function(value):

        perc_domain = (value - domain[0]) / domain_span
        place_in_range = perc_domain * range_span
        return place_in_range + range[0]

    return scale_function


def initiate_counters(record):
    record["num_corruptions"] = 0
    record["num_name_corruptions"] = 0
    record["num_location_corruptions"] = 0
    record["num_birth_place_corruptions"] = 0
    record["num_dob_corruptions"] = 0
    record["num_occupation_corruptions"] = 0
    record["num_gender_corruptions"] = 0
    return record
