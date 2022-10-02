def birth_country_gen_uncorrupted_record(formatted_master_record, record_to_modify={}):

    record_to_modify["birth_country"] = formatted_master_record["birth_countryLabel"]

    return record_to_modify
