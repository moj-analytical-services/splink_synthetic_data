# %%
import os
from scrape_wikidata.names import (
    SQL_GN_SAID_TO_BE_SAME_AS,
    SQL_FN_SAID_TO_BE_SAME_AS,
    SQL_GN_NICKNAME,
    SQL_GN_SHORTNAME,
    SQL_HYPOCORISM,
    get_standardised_table,
    get_diminutives,
    get_given_name_weighted_lookup,
    get_family_name_weighted_lookup,
)


from pathlib import Path

out_folder = "out_data/wikidata/raw/persons/by_dob"


BASE_PATH = "out_data/wikidata/raw/names/"
BASE_PATH_GN = os.path.join(BASE_PATH, "name_type=given")
BASE_PATH_FN = os.path.join(BASE_PATH, "name_type=family")

Path(BASE_PATH).mkdir(parents=True, exist_ok=True)
Path(BASE_PATH_GN).mkdir(parents=True, exist_ok=True)
Path(BASE_PATH_FN).mkdir(parents=True, exist_ok=True)

# %%

# Scrape first name said to be the same as
for page in range(0, 100, 1):
    pagesize = 5000
    path = os.path.join(
        BASE_PATH_GN,
        f"stbtsa_page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet",
    )

    if not os.path.exists(path):
        df = get_standardised_table(
            SQL_GN_SAID_TO_BE_SAME_AS, "said_to_be_the_same_as", page, pagesize
        )
        df = df.drop_duplicates()

        df.to_parquet(
            path,
            index=False,
        )


# %%


# Scrape first name said to be the same as
for page in range(0, 100, 1):
    pagesize = 5000

    path = os.path.join(
        BASE_PATH_FN,
        f"stbtsa_page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet",
    )

    if not os.path.exists(path):
        df = get_standardised_table(
            SQL_FN_SAID_TO_BE_SAME_AS, "said_to_be_the_same_as", page, pagesize
        )
        df = df.drop_duplicates()

        df.to_parquet(
            path,
            index=False,
        )


# %%

# Scrape given name said to be the same as
for page in range(0, 100, 1):
    pagesize = 5000
    path = os.path.join(
        BASE_PATH_GN,
        f"nickname_page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet",
    )

    if not os.path.exists(path):
        df = get_standardised_table(SQL_GN_NICKNAME, "nickname", page, pagesize)
        df = df.drop_duplicates()

        df.to_parquet(
            path,
            index=False,
        )

# %%

for page in range(0, 100, 1):
    pagesize = 5000
    path = os.path.join(
        BASE_PATH_GN,
        f"shortname_page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet",
    )

    if not os.path.exists(path):
        df = get_standardised_table(SQL_GN_SHORTNAME, "shortname", page, pagesize)
        df = df.drop_duplicates()

        df.to_parquet(
            path,
            index=False,
        )


# %%
for page in range(0, 100, 1):
    pagesize = 5000
    path = os.path.join(
        BASE_PATH_GN,
        f"hypo_page_{page}_{page*pagesize}_to_{(page+1)*pagesize-1}.parquet",
    )
    if not os.path.exists(path):

        df = get_standardised_table(SQL_HYPOCORISM, "hypocorism", page, pagesize)
        df = df.drop_duplicates()

        df.to_parquet(
            path,
            index=False,
        )

# %%
diminutives = get_diminutives()
path = os.path.join(
    BASE_PATH_GN,
    "diminutives.parquet",
)
diminutives.to_parquet(
    path,
    index=False,
)
