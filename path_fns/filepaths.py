import os

OUT_BASE = "out_data"
WIKIDATA = "wikidata"
RAW = "raw"
PERSONS = "persons"
NAMES = "names"


PERSONS_BY_DOD_RAW_OUT_PATH = os.path.join(OUT_BASE, WIKIDATA, RAW, PERSONS, "by_dod")
NAMES_RAW_OUT_PATH_BASE = os.path.join(OUT_BASE, WIKIDATA, RAW, NAMES)
NAMES_RAW_OUT_PATH_GIVEN_NAME = os.path.join(NAMES_RAW_OUT_PATH_BASE, "name_type=given")
NAMES_RAW_OUT_PATH_FAMILY_NAME = os.path.join(
    NAMES_RAW_OUT_PATH_BASE, "name_type=family"
)


def persons_by_dob_raw_filename_year_month(year, month):
    return os.path.join(PERSONS_BY_DOD_RAW_OUT_PATH, f"dod_{year}_{month:02}.parquet")


def persons_by_dob_raw_filename_full_year(year):
    return os.path.join(PERSONS_BY_DOD_RAW_OUT_PATH, f"dod_{year}_full.parquet")


# Processed
PROCESSED = "processed"
PERSONS_PROCESSED_ONE_ROW_PER_PERSON_DIR = os.path.join(
    OUT_BASE,
    WIKIDATA,
    PROCESSED,
    "one_row_per_person"
)
PERSONS_PROCESSED_ONE_ROW_PER_PERSON = os.path.join(
    PERSONS_PROCESSED_ONE_ROW_PER_PERSON_DIR,
    "raw_scraped_one_row_per_person.parquet"
)

NAMES_PROCESSED_GIVEN_NAME_ALT_LOOKUP = os.path.join(
    OUT_BASE, WIKIDATA, PROCESSED, "alt_name_lookups", "given_name_lookup.parquet"
)
NAMES_PROCESSED_FAMILY_NAME_ALT_LOOKUP = os.path.join(
    OUT_BASE, WIKIDATA, PROCESSED, "alt_name_lookups", "family_name_lookup.parquet"
)

# Transformed master data
TRANSFORMED = "transformed_master_data"
TRANSFORMED_MASTER_DATA = os.path.join(
    OUT_BASE,
    WIKIDATA,
    TRANSFORMED,
)

TRANSFORMED_MASTER_DATA_ONE_ROW_PER_PERSON = os.path.join(
    TRANSFORMED_MASTER_DATA, "one_row_per_person"
)
