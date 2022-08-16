from corrupt.geco_corrupt import (
    CorruptValueNumpad,
    CorruptValueQuerty,
    position_mod_uniform,
)


def string_corrupt_numpad(
    formatted_master_record,
    input_colname,
    output_colname,
    record_to_modify={},
    row_prob=0.5,
    col_prob=0.5,
):
    input_value = formatted_master_record[input_colname]
    if not input_value:
        record_to_modify[output_colname] = None
        return record_to_modify

    numpad_corruptor = CorruptValueNumpad(
        position_function=position_mod_uniform, row_prob=row_prob, col_prob=col_prob
    )
    input_value_as_str = str(input_value)
    record_to_modify[output_colname] = numpad_corruptor.corrupt_value(
        input_value_as_str
    )
    if record_to_modify[output_colname] != input_value_as_str:
        record_to_modify["num_" + output_colname + "_corruptions"] += 1
    return record_to_modify


def string_corrupt_querty_keyboard(
    formatted_master_record,
    input_colname,
    output_colname,
    record_to_modify={},
    row_prob=0.5,
    col_prob=0.5,
):
    input_value = formatted_master_record[input_colname]

    if not input_value:
        record_to_modify[output_colname] = None
        return record_to_modify

    querty_corruptor = CorruptValueQuerty(
        position_function=position_mod_uniform, row_prob=row_prob, col_prob=col_prob
    )

    input_value_as_str = str(input_value)
    record_to_modify[output_colname] = querty_corruptor.corrupt_value(
        input_value_as_str
    )
    if record_to_modify[output_colname] != input_value_as_str:
        record_to_modify["num_" + output_colname + "_corruptions"] += 1
    return record_to_modify
