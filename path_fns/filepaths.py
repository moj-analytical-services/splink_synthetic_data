import os

OUT_BASE = "out_data"
WIKIDATA = "wikidata"
RAW = "raw"
PERSONS = "persons"


PERSONS_BY_DOB_RAW_OUT_PATH = os.path.join(OUT_BASE, WIKIDATA, RAW, PERSONS, "by_dod")


def persons_by_dob_raw_filename(year, month):
    return os.path.join(PERSONS_BY_DOB_RAW_OUT_PATH, f"dod_{year}_{month:02}.parquet")
