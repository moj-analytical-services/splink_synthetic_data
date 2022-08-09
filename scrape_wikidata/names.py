import requests
from scrape_wikidata.query_wikidata import replace_url, query_with_offset
import pandas as pd
import duckdb


SQL_GN_SAID_TO_BE_SAME_AS = """\
SELECT ?given_name ?given_nameLabel ?said_to_be_the_same_asLabel
WITH {
    SELECT ?given_name ?said_to_be_the_same_as WHERE {
    ?given_name (wdt:P31/(wdt:P279*)) wd:Q202444;
        wdt:P460 ?said_to_be_the_same_as.
    }
    LIMIT 100
}
as %results

WHERE {
  INCLUDE %results.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
"""

SQL_FN_SAID_TO_BE_SAME_AS = """
SELECT ?family_name ?family_nameLabel ?said_to_be_the_same_asLabel
WITH {
    SELECT ?family_name  ?said_to_be_the_same_as ?language  {
    ?family_name (wdt:P31/(wdt:P279*)) wd:Q101352.
    ?family_name wdt:P460 ?said_to_be_the_same_as.
    }
    LIMIT 100
} as %results
WHERE {
  INCLUDE %results.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
"""


SQL_GN_NICKNAME = """\
SELECT ?given_name ?given_nameLabel ?nickname
WITH {
    SELECT ?given_name ?nickname ?language WHERE {
    ?given_name (wdt:P31/(wdt:P279*)) wd:Q202444;
        wdt:P1449 ?nickname.
    }
    LIMIT 100
}
as %results

WHERE {
  INCLUDE %results.get_given_name_weighted_lookup
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
"""


SQL_GN_SHORTNAME = """\
SELECT ?given_name ?given_nameLabel ?shortnameLabel
WITH {
    SELECT ?given_name ?shortname ?language WHERE {
    ?given_name (wdt:P31/(wdt:P279*)) wd:Q202444;
        wdt:P460 ?shortname.
    }
    LIMIT 100
}
as %results

WHERE {
  INCLUDE %results.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
"""


SQL_HYPOCORISM = """
SELECT ?of ?ofLabel ?hypocorismLabel   WHERE {
    ?hypocorism wdt:P31 wd:Q1130279;
        p:P31 ?statement.
    ?statement pq:P642 ?of.
    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
LIMIT 100
"""

RENAMES = {
    "shortnameLabel": "alt_name",
    "nickname": "alt_name",
    "shortnameLabel": "alt_name",
    "said_to_be_the_same_asLabel": "alt_name",
    "given_nameLabel": "original_name",
    "family_nameLabel": "original_name",
    "of": "given_name",
    "ofLabel": "original_name",
    "hypocorismLabel": "alt_name",
}


def get_standardised_table(query, name_variant, page=0, pagesize=5000):

    df = query_with_offset(
        query,
        page,
        pagesize,
    )
    df["name_variant_type"] = name_variant

    df = df.rename(columns=RENAMES)
    df = df.applymap(replace_url)

    if len(df.columns) == 4:
        return df
    else:
        raise ValueError("df does not contain 4 cols")


def get_diminutives():
    url1 = "https://raw.githubusercontent.com/jonathanhar/diminutives.db/master/male_diminutives.csv"
    url2 = "https://raw.githubusercontent.com/jonathanhar/diminutives.db/master/female_diminutives.csv"

    urls = [url1, url2]
    rows = []
    for url in urls:
        r = requests.get(url)
        for line in r.text.splitlines():
            elems = line.split(",")
            original_name = elems.pop(0)
            for e in elems:
                row = {
                    "given_name_id": None,
                    "original_name": original_name,
                    "alt_name": e,
                    "name_variant_type": "diminutive",
                }
                rows.append(row)

    return pd.DataFrame(rows)


def get_given_name_weighted_lookup(con):
    sql = """

    with names_concat as (
        select lower(original_name) as original_name, lower(alt_name) as alt_name, name_variant_type
        from names
        union all
        select lower(alt_name) as original_name, lower(original_name) as alt_name, name_variant_type
        from names

        ),

    distinct_names_concat as (
        select distinct original_name, alt_name, name_variant_type
        from names_concat
    ),

    weighted as (

        select n.*, count,

        case
            when name_variant_type = 'nickname' then count+5000
            when name_variant_type = 'diminutive' then count+2000
            when name_variant_type = 'hypocorism' then count+2000
            else  count
            end as weighted_count

        from distinct_names_concat as n
        inner join
        counts as c
        on n.alt_name = c.given_name
        where
        count >= 10
        and original_name != alt_name
    )

    select original_name, alt_name, sum(weighted_count) as weight
    from weighted
    group by original_name, alt_name
    order by original_name, weight desc
    """

    weights = con.query(sql).to_df()

    sql = """

    with sums as (
        select original_name, sum(weight) as sumweight
        from weights
        group by original_name)

    select w.original_name, w.alt_name, w.weight/s.sumweight as weight
    from weights as w
    left join sums as s
    on w.original_name = s.original_name

    """
    return con.query(sql).to_df()


def get_family_name_weighted_lookup(names, counts):

    sql = """
    with names_concat as (
        select lower(original_name) as original_name, lower(alt_name) as alt_name, name_variant_type
        from names
        union all
        select lower(alt_name) as original_name, lower(original_name) as alt_name, name_variant_type
        from names

        ),

    distinct_names_concat as (
        select distinct original_name, alt_name, name_variant_type
        from names_concat
    ),

    weighted as (
        select n.*, count
        from distinct_names_concat as n
        inner join
        counts as c
        on n.alt_name = c.surname
        where
        count >= 10
        and original_name != alt_name
    )

    select
        original_name,
        alt_name,
        sum(count) as weight
    from weighted
    group by original_name, alt_name
    order by original_name, sum(count)  desc
    """

    weights = duckdb.query(sql).to_df()

    sql = """

    with sums as (
        select original_name, sum(weight) as sumweight
        from weights
        group by original_name)

    select w.original_name, w.alt_name, w.weight/s.sumweight as weight
    from weights as w
    left join sums as s
    on w.original_name = s.original_name



    """
    return duckdb.query(sql).to_df()
