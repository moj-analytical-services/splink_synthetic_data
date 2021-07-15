import numpy as np


def country_citizenship_exact_match(master_record, corrupted_record={}):

    if master_record["country_citizenship"] is None:
        corrupted_record["citizenship"] = None
        return corrupted_record

    corrupted_record["citizenship"] = master_record["country_citizenship"][0]

    return corrupted_record


def country_citizenship_random(master_record, corrupted_record={}):

    if master_record["country_citizenship"] is None:
        corrupted_record["citizenship"] = None
        return corrupted_record

    corrupted_record["citizenship"] = np.random.choice(
        master_record["country_citizenship"]
    )

    return corrupted_record
