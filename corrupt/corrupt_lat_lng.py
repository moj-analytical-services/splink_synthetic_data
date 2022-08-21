def lat_lng_corrupt_distance(
    formatted_master_record, input_colname, output_colname, record_to_modify={}
):

    if not formatted_master_record[input_colname]:
        record_to_modify[output_colname] = None
        return record_to_modify
    else:
        record_to_modify[output_colname] = formatted_master_record[input_colname]
    return record_to_modify


# raw_data.iloc[3]["birth_coordinates"][0]


def lat_lng_uncorrupted_record(
    formatted_master_record, input_colname, output_colname, record_to_modify={}
):
    record_to_modify[output_colname] = str(formatted_master_record[input_colname])
    return record_to_modify
