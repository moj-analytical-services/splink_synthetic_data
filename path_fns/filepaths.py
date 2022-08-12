import os

OUT_BASE = "out_data"
WIKIDATA = "wikidata"
RAW = "raw"
PERSONS = "persons"
NAMES = "names"


PERSONS_BY_DOB_RAW_OUT_PATH = os.path.join(OUT_BASE, WIKIDATA, RAW, PERSONS, "by_dod")
NAMES_RAW_OUT_PATH_BASE = os.path.join(OUT_BASE, WIKIDATA, RAW, NAMES)
NAMES_RAW_OUT_PATH_GIVEN_NAME = os.path.join(NAMES_RAW_OUT_PATH_BASE, "name_type=given")
NAMES_RAW_OUT_PATH_FAMILY_NAME = os.path.join(
    NAMES_RAW_OUT_PATH_BASE, "name_type=family"
)


def persons_by_dob_raw_filename(year, month):
    return os.path.join(PERSONS_BY_DOB_RAW_OUT_PATH, f"dod_{year}_{month:02}.parquet")
