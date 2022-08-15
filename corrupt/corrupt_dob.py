def dob_format_master_record(master_input_record):
    if not master_input_record["dob"]:
        master_input_record["dob"] = None
    else:
        master_input_record["dob"] = master_input_record["dob"][0]
    return master_input_record


def dob_gen_uncorrupted_record(formatted_master_record, input_record={}):
    input_record["dob"] = formatted_master_record["dob"]
    return input_record
