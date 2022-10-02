import argparse
from pathlib import Path
import os
import logging
import pandas as pd
import numpy as np
import duckdb


from corrupt.corruption_functions import (
    master_record_no_op,
    format_master_data,
    generate_uncorrupted_output_record,
    format_master_record_first_array_item,
    null_corruption,
)


from corrupt.corrupt_occupation import (
    occupation_format_master_record,
    occupation_gen_uncorrupted_record,
    occupation_corrupt,
)

from corrupt.corrupt_name import (
    full_name_gen_uncorrupted_record,
    full_name_alternative,
    each_name_alternatives,
    name_inversion,
    full_name_typo,
)


from corrupt.corrupt_date import (
    date_corrupt_timedelta,
    date_gen_uncorrupted_record,
    date_corrupt_jan_first,
)

from corrupt.corrupt_country_citizenship import (
    country_citizenship_format_master_record,
    country_citizenship_corrupt,
    country_citizenship_gen_uncorrupted_record,
)


from corrupt.corrupt_birth_country import birth_country_gen_uncorrupted_record


from path_fns.filepaths import (
    TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON,
    FINAL_CORRUPTED_OUTPUT_FILES_BASE,
)

from corrupt.corrupt_lat_lng import lat_lng_uncorrupted_record, lat_lng_corrupt_distance

from functools import partial
from corrupt.geco_corrupt import get_zipf_dist


from corrupt.record_corruptor import (
    CompositeCorruption,
    ProbabilityAdjustmentFromLookup,
    RecordCorruptor,
    ProbabilityAdjustmentFromSQL,
)


logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(message)s",
)
logger.setLevel(logging.INFO)


con = duckdb.connect()

in_path = os.path.join(
    TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON, "transformed_master_data.parquet"
)


# Configure how corruptions will be made for each field

# Col name is the OUTPUT column name.  For instance, we may input given name,
# family name etc to output full_name

# Guide to keys:
# format_master_data.  This functino may apply additional cleaning to the master
# record.  The same formatted master ata is then available to the
# 'gen_uncorrupted_record' and 'corruption_functions'


config = [
    {
        "col_name": "full_name",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": full_name_gen_uncorrupted_record,
    },
    {
        "col_name": "birth_country",
        "format_master_data": partial(
            format_master_record_first_array_item, colname="birth_countryLabel"
        ),
        "gen_uncorrupted_record": birth_country_gen_uncorrupted_record,
    },
    {
        "col_name": "occupation",
        "format_master_data": occupation_format_master_record,
        "gen_uncorrupted_record": occupation_gen_uncorrupted_record,
    },
    {
        "col_name": "dob",
        "format_master_data": partial(
            format_master_record_first_array_item, colname="dob"
        ),
        "gen_uncorrupted_record": partial(
            date_gen_uncorrupted_record, input_colname="dob", output_colname="dob"
        ),
    },
    {
        "col_name": "dod",
        "format_master_data": partial(
            format_master_record_first_array_item, colname="dod"
        ),
        "gen_uncorrupted_record": partial(
            date_gen_uncorrupted_record, input_colname="dod", output_colname="dod"
        ),
    },
    {
        "col_name": "birth_coordinates",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": partial(
            lat_lng_uncorrupted_record,
            input_colname="birth_coordinates",
            output_colname="birth_coordinates",
        ),
    },
    {
        "col_name": "country_citizenLabel",
        "format_master_data": country_citizenship_format_master_record,
        "gen_uncorrupted_record": country_citizenship_gen_uncorrupted_record,
    },
]


rc = RecordCorruptor()


########
# Date of birth and date of death corruptions
########

# Create a timedelta corruption with baseline probability 20%
# This is a simple independent corruption function that's not affected
# by the presence or absence of other corruptions, or the values in the data
rc.add_simple_corruption(
    name="dob_timedelta",  # So we can keep a list of the corruptions that were activated
    corruption_function=date_corrupt_timedelta,  # A python function containing the definition of the function
    args={
        "input_colname": "dob",
        "output_colname": "dob",
        "num_days_delta": 50,
    },  # Any arguments that need to be passed to the python function
    baseline_probability=0.1,
)

# A corruption function that simultaneously sets dob and dod to jan first
# We will also add a probability adjustment that modifies the probability
# of activation based on the values in the record
dob_dod_jan_first = CompositeCorruption(
    name="dob_dod_jan_first_corruption", baseline_probability=0.1
)
dob_dod_jan_first.add_corruption_function(
    date_corrupt_jan_first, args={"input_colname": "dob", "output_colname": "dob"}
)
dob_dod_jan_first.add_corruption_function(
    date_corrupt_jan_first, args={"input_colname": "dod", "output_colname": "dod"}
)

rc.add_composite_corruption(dob_dod_jan_first)

# Make this twice as likely if the dob is < 1990
sql_condition = "year(try_cast(dob as date)) < 1900"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, dob_dod_jan_first, 4)
rc.add_probability_adjustment(adjustment)

rc.add_simple_corruption(
    name="dob_null",
    corruption_function=null_corruption,
    args={"output_colname": "dob"},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="dod_null",
    corruption_function=null_corruption,
    args={"output_colname": "dod"},
    baseline_probability=0.1,
)

# Note that the order in which you register corruptions is the order in which they're
# executed.  e.g. you might want the jan first corruption to come after the timedelta
# corruption, which may be better in the case where both are activated


########
# Name-based corruptions
########


rc.add_simple_corruption(
    name="pick_alt_full_name",
    corruption_function=full_name_alternative,
    args={},
    baseline_probability=0.4,
)

rc.add_simple_corruption(
    name="pick_alternative_individual_names",
    corruption_function=each_name_alternatives,
    args={},
    baseline_probability=0.1,
)

# Name inversions more common for certain birthCountries
name_inversion_corrpution = CompositeCorruption(
    name="name_inversion", baseline_probability=0.05
)
name_inversion_corrpution.add_corruption_function(name_inversion, args={})
rc.add_composite_corruption(name_inversion_corrpution)

adjustment_lookup = {
    "birth_country": {
        "Japan": [(name_inversion_corrpution, 2)],
        "People's Republic of China": [(name_inversion_corrpution, 4)],
    }
}
adjustment = ProbabilityAdjustmentFromLookup(adjustment_lookup)
rc.add_probability_adjustment(adjustment)


# Typos more common from certain contries
name_typo_corruption = CompositeCorruption(name="name_typo", baseline_probability=0.2)
name_typo_corruption.add_corruption_function(full_name_typo, args={})
rc.add_composite_corruption(name_typo_corruption)


sql_condition = "birth_country not in ('United States of America', 'United Kingdom') and birth_country not null"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, name_typo_corruption, 2)
rc.add_probability_adjustment(adjustment)

rc.add_simple_corruption(
    name="full_name_null",
    corruption_function=null_corruption,
    args={"output_colname": "full_name"},
    baseline_probability=0.1,
)

########
# Occupation corruption
########

rc.add_simple_corruption(
    name="occupation_corrupt",
    corruption_function=occupation_corrupt,
    args={},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="occupation_null",
    corruption_function=null_corruption,
    args={"output_colname": "occupation"},
    baseline_probability=0.1,
)

########
# Country citizenship corruption
########
rc.add_simple_corruption(
    name="country_citizenship_corrupt",
    corruption_function=country_citizenship_corrupt,
    args={},
    baseline_probability=0.3,
)

rc.add_simple_corruption(
    name="country_citizenship_null",
    corruption_function=null_corruption,
    args={"output_colname": "country_citizenship"},
    baseline_probability=0.1,
)

########
# Birth coordinates corruption
########


rc.add_simple_corruption(
    name="birth_coordinates_corrupt",
    corruption_function=lat_lng_corrupt_distance,
    args={
        "input_colname": "birth_coordinates",
        "output_colname": "birth_coordinates",
        "distance_min": 25,
        "distance_max": 25,
    },
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="birth_coordinates_null",
    corruption_function=null_corruption,
    args={"output_colname": "birth_coordinates"},
    baseline_probability=0.1,
)


max_corrupted_records = 20
zipf_dist = get_zipf_dist(max_corrupted_records)


pd.options.display.max_columns = 1000
pd.options.display.max_colwidth = 1000

Path(FINAL_CORRUPTED_OUTPUT_FILES_BASE).mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="data_linking job runner")

    parser.add_argument("--start_year", type=int)
    parser.add_argument("--num_years", type=int)
    args = parser.parse_args()
    start_year = args.start_year
    num_years = args.num_years

    for year in range(start_year, start_year + num_years + 1):

        out_path = os.path.join(FINAL_CORRUPTED_OUTPUT_FILES_BASE, f"{year}.parquet")

        if os.path.exists(out_path):
            continue

        sql = f"""
        select *
        from '{in_path}'
        where
            year(try_cast(dod[1] as date)) = {year}
        """

        raw_data = con.execute(sql).df()
        records = raw_data.to_dict(orient="records")

        output_records = []
        for i, master_input_record in enumerate(records):

            # Formats the input data into an easy format for producing
            # an uncorrupted/corrupted outputs records
            formatted_master_record = format_master_data(master_input_record, config)

            uncorrupted_output_record = generate_uncorrupted_output_record(
                formatted_master_record, config
            )
            uncorrupted_output_record["corruptions_applied"] = []

            output_records.append(uncorrupted_output_record)

            # How many corrupted records to generate
            total_num_corrupted_records = np.random.choice(
                zipf_dist["vals"], p=zipf_dist["weights"]
            )

            for i in range(total_num_corrupted_records):
                record_to_modify = uncorrupted_output_record.copy()
                record_to_modify["corruptions_applied"] = []
                record_to_modify["id"] = (
                    uncorrupted_output_record["cluster"] + f"_{i+1}"
                )
                record_to_modify["uncorrupted_record"] = False
                rc.apply_probability_adjustments(uncorrupted_output_record)
                corrupted_record = rc.apply_corruptions_to_record(
                    formatted_master_record,
                    record_to_modify,
                )
                output_records.append(corrupted_record)

        df = pd.DataFrame(output_records)

        df.to_parquet(out_path, index=False)
        print(f"written {year} with {len(df):,.0f} records")
