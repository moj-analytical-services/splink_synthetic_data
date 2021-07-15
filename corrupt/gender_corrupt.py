def gender_exact_match(master_record, corrupted_record={}):
    corrupted_record["gender"] = master_record["gender"]
    return corrupted_record


def gender_corrupt(master_record, corrupted_record={}):
    if master_record["gender"] == "male":
        corrupted_record["gender"] = "female"

    if master_record["gender"] == "female":
        corrupted_record["gender"] = "male"

    return corrupted_record
