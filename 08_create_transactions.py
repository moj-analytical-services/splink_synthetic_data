import pandas as pd
import duckdb
import os
import numpy as np
import random
from uuid import uuid4
from datetime import timedelta
import datetime

from path_fns.filepaths import TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON

pd.options.display.max_columns = 1000

con = duckdb.connect()

in_path = os.path.join(
    TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON, "transformed_master_data.parquet"
)


sql = f"""
select *
from '{in_path}'
where
array_length(occupation) > 0

and array_length(humanAltLabel) > 0
and array_length(residence) > 0


limit 3000
"""


raw_data = con.execute(sql).df()
records = raw_data.to_dict(orient="records")


def random_date(start, end):
    start = datetime.datetime.fromisoformat(start).date()
    end = datetime.datetime.fromisoformat(end).date()
    start_ordinal = start.toordinal()
    end_ordinal = end.toordinal() + 1

    values = list(range(start_ordinal, end_ordinal))
    skew = list(range(1, 1 + end_ordinal - start_ordinal))

    total = sum(skew)
    skew_p = [v / total for v in skew]
    chosen_ordinal = np.random.choice(values, p=skew_p)

    return datetime.date.fromordinal(chosen_ordinal)


def random_amount():
    mu, sigma = 5.0, 1.0  # mean and standard deviation
    s = np.random.lognormal(mu, sigma, 1)
    return round(s[0], 2)


def random_exchange_rate():
    currencies = [("USD", 1.16), ("EUR", 1.16), ("JPY", 161.27), ("GBP", 1.00)]

    (currency, xr) = random.choice(currencies)
    perturb = np.random.normal(1, 0.03)
    if currency != "GBP":
        xr_realised = xr * perturb
    else:
        xr_realised = 1.0

    return (currency, xr, xr_realised)


def random_payment_reference():
    ref = ["money", "payment", "donation", "", str(uuid4())[:8]]
    return random.choice(ref)


def random_payment_memo(raw_record):

    hl = raw_record["humanLabel"][0]

    new_parts = []
    for part in hl.split(" "):
        if random.uniform(0, 1) < 0.7:
            new_parts.append(part[:1])
        else:
            new_parts.append(part)
    person = " ".join(new_parts)
    return f"{person}".strip().upper()


def random_payment_type():
    a = ["BGC", "CHQ", "CSH", "WRE"]
    return np.random.choice(a, p=[0.6, 0.2, 0.1, 0.1])


data_to_corrupt = []

record_id = 0
for person_record in records:
    for i in range(np.random.randint(1, 30)):
        this_record = {"unique_id": record_id}
        record_id += 1
        this_record["transaction_date"] = random_date("2022-01-01", "2022-05-10")
        currency, xr, xr_realised = random_exchange_rate()
        this_record["currency"] = currency
        this_record["exchange_rate_average"] = xr
        this_record["exchange_rate_transaction"] = xr_realised
        this_record["amount"] = random_amount()
        this_record["memo"] = random_payment_memo(person_record)
        this_record["ref"] = random_payment_reference()
        this_record["type"] = random_payment_type()

        data_to_corrupt.append(this_record)
master_data = pd.DataFrame(data_to_corrupt)
master_data

master_data_as_dict = master_data.to_dict(orient="records")


from functools import partial
from corrupt.corruption_functions import (
    master_record_no_op,
    basic_null_fn,
    format_master_data,
)

from corrupt.error_vector import generate_error_vectors, apply_error_vector


def generate_uncorrupted_output_record(formatted_master_record, config):

    uncorrupted_record = {"uncorrupted_record": True}

    uncorrupted_record["ground_truth"] = formatted_master_record["unique_id"]

    for c in config:
        fn = c["gen_uncorrupted_record"]
        uncorrupted_record = fn(
            formatted_master_record, record_to_modify=uncorrupted_record
        )

    uncorrupted_record["unique_id"] = formatted_master_record["unique_id"]

    return uncorrupted_record


def uncorrupted_record_no_op(formatted_master_record, column, record_to_modify={}):
    record_to_modify[column] = formatted_master_record[column]
    return record_to_modify


def null_no_op(formatted_master_record, column, record_to_modify={}):
    record_to_modify[column] = formatted_master_record[column]
    return record_to_modify


def date_corrupt_timedelta(
    formatted_master_record, input_colname, output_colname, record_to_modify={}
):

    input_value = formatted_master_record[input_colname]

    choice = np.random.choice(["small", "medium", "large"], p=[0.7, 0.2, 0.1])

    if choice == "small":
        delta = timedelta(days=random.randint(1, 2))
    elif choice == "medium":
        delta = timedelta(days=random.randint(1, 6))
    elif choice == "large":
        delta = timedelta(days=random.randint(1, 21))

    input_value = input_value + delta
    record_to_modify[output_colname] = input_value
    return record_to_modify


def amount_uncorrupted(formatted_master_record, record_to_modify={}):
    record_to_modify["amount"] = (
        float(formatted_master_record["amount"])
        * formatted_master_record["exchange_rate_transaction"]
    )
    record_to_modify["amount"] = round(record_to_modify["amount"], 2)

    return record_to_modify


def amount_corrupted(formatted_master_record, record_to_modify={}):
    record_to_modify["amount"] = (
        float(formatted_master_record["amount"])
        * formatted_master_record["exchange_rate_average"]
    )
    record_to_modify["amount"] = round(record_to_modify["amount"], 2)

    return record_to_modify


def memo_uncorrupt(formatted_master_record, record_to_modify={}):
    memo = formatted_master_record["memo"]
    ref = formatted_master_record["ref"]
    type = formatted_master_record["type"]

    if random.uniform(0, 1) < 0.9:
        memo = f"{memo} {ref}"

    if random.uniform(0, 1) < 0.9:
        memo = f"{memo} {type}"

    if random.uniform(0, 1) < 0.9:
        memo = memo[:15]

    record_to_modify["memo"] = memo
    return record_to_modify


def memo_corrupt(formatted_master_record, record_to_modify={}):
    memo = formatted_master_record["memo"]
    ref = formatted_master_record["ref"]
    type = formatted_master_record["type"]

    if random.uniform(0, 1) < 0.7:
        memo = f"{memo} {ref}"

    if random.uniform(0, 1) < 0.7:
        memo = f"{memo} {type}"

    if random.uniform(0, 1) < 0.7:
        memo = memo[:15]

    record_to_modify["memo"] = memo
    return record_to_modify


config = [
    {
        "col_name": "memo",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": memo_uncorrupt,
        "corruption_functions": [
            {
                "fn": memo_corrupt,
                "p": 1.0,
            },
        ],
        "null_function": partial(null_no_op, column="transaction_date"),
    },
    {
        "col_name": "transaction_date",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": partial(
            uncorrupted_record_no_op, column="transaction_date"
        ),
        "corruption_functions": [
            {
                "fn": partial(
                    date_corrupt_timedelta,
                    input_colname="transaction_date",
                    output_colname="transaction_date",
                ),
                "p": 1.0,
            },
        ],
        "null_function": partial(null_no_op, column="transaction_date"),
    },
    {
        "col_name": "amount_gbp",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": amount_uncorrupted,
        "corruption_functions": [
            {
                "fn": amount_corrupted,
                "p": 1.0,
            },
        ],
        "null_function": partial(null_no_op, column="amount_gbp"),
    },
]

output_records = []
for i, master_input_record in enumerate(master_data_as_dict):

    # Formats the input data into an easy format for producing
    # an uncorrupted/corrupted outputs records
    formatted_master_record = format_master_data(master_input_record, config)

    uncorrupted_output_record = generate_uncorrupted_output_record(
        formatted_master_record, config
    )

    output_records.append(uncorrupted_output_record)

    # How many corrupted records to generate
    total_num_corrupted_records = 1

    # Decide what types of corruptions to introduce
    error_vectors = generate_error_vectors(config, total_num_corrupted_records)
    # Apply corruptions
    for vector in error_vectors:
        corrupted_record = apply_error_vector(vector, formatted_master_record, config)
        corrupted_record["uncorrupted_record"] = False
        corrupted_record["ground_truth"] = formatted_master_record["unique_id"]
        corrupted_record["unique_id"] = formatted_master_record["unique_id"]
        output_records.append(corrupted_record)


df = pd.DataFrame(output_records)

f1 = df["uncorrupted_record"] == True
df_left = df[f1].copy()
df_left = df_left.drop("uncorrupted_record", axis=1)
df_left.to_parquet("transactions_left.parquet", index=False)
df_left


f2 = df["uncorrupted_record"] == False
df_right = df[f2].copy()
df_right = df_right.drop("uncorrupted_record", axis=1)
df_right.to_parquet("transactions_right.parquet", index=False)
df_right

print(len(df_right))
df_right
