from functools import partial
import random
import numpy as np


def master_record_no_op(master_record):
    return master_record


def _basic_null_fn_to_partial(master_record, col_name, null_prob, record_to_modify={}):

    if random.uniform(0, 1) < null_prob:
        record_to_modify[col_name] = None

    return record_to_modify


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


def format_master_data(master_input_record, config):
    for c in config:
        fn = c["format_master_data"]
        master_record = fn(master_input_record)
    return master_record


def generate_uncorrupted_output_record(formatted_master_record, config):

    uncorrupted_record = {"uncorrupted_record": True}

    uncorrupted_record["cluster"] = formatted_master_record["human"]

    for c in config:
        fn = c["gen_uncorrupted_record"]
        uncorrupted_record = fn(
            formatted_master_record, record_to_modify=uncorrupted_record
        )

    uncorrupted_record["id"] = formatted_master_record["human"] + "_0"

    return uncorrupted_record


def get_null_prob(counter, group_size, config):
    null_domain = [0, group_size - 1]
    null_range = [config["start_prob_null"], config["end_prob_null"]]
    null_scale = scale_linear(null_domain, null_range)
    prob_null = null_scale(counter)
    return prob_null


def get_prob_of_corruption(counter, group_size, config):
    prob_corrupt_domain = [0, group_size - 1]
    prob_corrupt_range = [config["start_prob_corrupt"], config["end_prob_corrupt"]]
    prob_corrupt_scale = scale_linear(prob_corrupt_domain, prob_corrupt_range)
    prob_corrupt = prob_corrupt_scale(counter)
    return prob_corrupt


def choose_corruption_function(config):
    weights = [f["p"] for f in config["corruption_functions"]]
    fns = [f["fn"] for f in config["corruption_functions"]]
    return np.random.choice(fns, p=weights)


def generate_corrupted_output_records(
    formatted_master_record,
    counter,
    total_num_corrupted_records,
    config,
):

    corrupted_record = {
        "uncorrupted_record": False,
    }

    corrupted_record["cluster"] = formatted_master_record["human"]
    corrupted_record["id"] = formatted_master_record["human"] + f"_{counter+1}"

    for c in config:

        corrupted_record["uncorrupted_record"] = False

        prob_null = get_null_prob(counter, total_num_corrupted_records, c)
        prob_corrupt = get_prob_of_corruption(counter, total_num_corrupted_records, c)

        # Choose corruption function to apply
        corruption_function = choose_corruption_function(c)

        if random.uniform(0, 1) < prob_corrupt:
            fn = corruption_function
            corrupted_record = fn(
                formatted_master_record, record_to_modify=corrupted_record
            )
        else:
            fn = c["gen_uncorrupted_record"]
            corrupted_record = fn(
                formatted_master_record, record_to_modify=corrupted_record
            )

        if random.uniform(0, 1) < prob_null:
            null_fn = c["null_function"]
            corrupted_record = null_fn(
                formatted_master_record,
                null_prob=prob_null,
                record_to_modify=corrupted_record,
            )

    return corrupted_record


def format_master_record_first_array_item(master_input_record, colname):
    if not master_input_record[colname]:
        master_input_record[colname] = None
    else:
        master_input_record[colname] = master_input_record[colname][0]
    return master_input_record
