import pyarrow.parquet as pq
from transform_master_data.alt_name_lookups import (
    get_name_weighted_lookup,
)
import duckdb
from path_fns.filepaths import (
    NAMES_RAW_OUT_PATH_GIVEN_NAME,
    NAMES_RAW_OUT_PATH_FAMILY_NAME,
    PERSONS_PROCESSED_ONE_ROW_PER_PERSON,
    NAMES_PROCESSED_GIVEN_NAME_ALT_LOOKUP,
    NAMES_PROCESSED_FAMILY_NAME_ALT_LOOKUP,
)

# Use the alternative names in out_data/wikidata/raw/names
# to create lookups like.  These can then be fed to numpy np.random.choice(names, p=weights)
# to pick alternative names

# | original_name   | alt_name_arr                      | alt_name_weight_arr      |
# |:----------------|:----------------------------------|:-------------------------|
# | jody            | ['joseph', 'joe', 'judith', 'jo'] | [0.43, 0.23, 0.16, 0.16] |

con = duckdb.connect()

alt_names_given = pq.read_table(NAMES_RAW_OUT_PATH_GIVEN_NAME)
con.register("alt_names_given", alt_names_given)


weighted_lookup_given_name = get_name_weighted_lookup(
    con,
    "given_nameLabel",
    "alt_names_given",
    f"'{PERSONS_PROCESSED_ONE_ROW_PER_PERSON}'",
)
weighted_lookup_given_name = weighted_lookup_given_name.fetch_arrow_table()


pq.write_table(weighted_lookup_given_name, NAMES_PROCESSED_GIVEN_NAME_ALT_LOOKUP)


alt_names_family = pq.read_table(NAMES_RAW_OUT_PATH_FAMILY_NAME)
con.register("alt_names_family", alt_names_family)
weighted_lookup_family_name = get_name_weighted_lookup(
    con,
    "family_nameLabel",
    "alt_names_family",
    f"'{PERSONS_PROCESSED_ONE_ROW_PER_PERSON}'",
)
weighted_lookup_family_name = weighted_lookup_family_name.fetch_arrow_table()

pq.write_table(weighted_lookup_family_name, NAMES_PROCESSED_FAMILY_NAME_ALT_LOOKUP)
