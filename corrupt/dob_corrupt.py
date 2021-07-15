from corrupt.geco_corrupt import (
    CorruptValueNumpad,
    position_mod_uniform,
)


def dob_exact_match(master_record, corrupted_record={}):
    corrupted_record["dob"] = master_record["dob"]
    return corrupted_record


# TODO: corruption based on distance in time
def dob_corrupt_typo(master_record, corrupted_record={}):

    if master_record["dob"] is None:
        corrupted_record["dob"] = None
        return corrupted_record

    numpad_corruptor = CorruptValueNumpad(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )
    dob = master_record["dob"]
    dob_ex_year = master_record["dob"][2:]
    corrupted_dob_ex_year = numpad_corruptor.corrupt_value(dob_ex_year)
    corrupted_record["dob"] = dob[:2] + corrupted_dob_ex_year
    return corrupted_record
