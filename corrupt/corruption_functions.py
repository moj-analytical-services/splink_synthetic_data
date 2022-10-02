from functools import partial


def master_record_no_op(master_record):
    return master_record


def _basic_null_fn_to_partial(master_record, col_name, record_to_modify={}):

    record_to_modify[col_name] = None

    return record_to_modify


def basic_null_fn(colname):
    return partial(_basic_null_fn_to_partial, col_name=colname)


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


def format_master_record_first_array_item(master_input_record, colname):
    if not master_input_record[colname]:
        master_input_record[colname] = None
    else:
        master_input_record[colname] = master_input_record[colname][0]
    return master_input_record


def null_corruption(formatted_master_record, record_to_modify, output_colname):
    record_to_modify[output_colname] = None
    return record_to_modify
