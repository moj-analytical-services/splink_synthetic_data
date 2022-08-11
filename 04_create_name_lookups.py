import pyarrow.parquet as pq
from transform_master_data.alt_name_lookups import (
    get_name_weighted_lookup,
)
import duckdb

# Use the alternative names in out_data/wikidata/raw/names
# to create lookups like.  These can then be fed to numpy np.random.choice(names, p=weights)
# to pick alternative names

# | original_name   | alt_name_arr                      | alt_name_weight_arr      |
# |:----------------|:----------------------------------|:-------------------------|
# | jody            | ['joseph', 'joe', 'judith', 'jo'] | [0.43, 0.23, 0.16, 0.16] |

con = duckdb.connect()

alt_names_given = pq.read_table("out_data/wikidata/raw/names/name_type=given/")
con.register("alt_names_given", alt_names_given)
weighted_lookup_given_name = get_name_weighted_lookup(
    con, "given_nameLabel", "alt_names_given", "'tidied.parquet'"
)
weighted_lookup_given_name = weighted_lookup_given_name.fetch_arrow_table()

out_path = "out_data/wikidata/processed/alt_name_lookups/given_name_lookup.parquet"
pq.write_table(weighted_lookup_given_name, out_path)


alt_names_family = pq.read_table("out_data/wikidata/raw/names/name_type=family/")
con.register("alt_names_family", alt_names_family)
weighted_lookup_family_name = get_name_weighted_lookup(
    con, "family_nameLabel", "alt_names_family", "'tidied.parquet'"
)
weighted_lookup_family_name = weighted_lookup_family_name.fetch_arrow_table()
out_path = "out_data/wikidata/processed/alt_name_lookups/family_name_lookup.parquet"
pq.write_table(weighted_lookup_family_name, out_path)
