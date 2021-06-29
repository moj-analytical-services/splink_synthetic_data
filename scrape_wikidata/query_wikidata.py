from SPARQLWrapper import SPARQLWrapper, JSON
import sys
import pandas as pd
import duckdb

endpoint_url = "https://query.wikidata.org/sparql"

QUERY_HUMAN = """
SELECT
    ?human
    ?humanLabel
    ?humanAltLabel
    ?dob
    ?birth_name
    ?given_name
    ?given_nameLabel
    ?family_name
    ?family_nameLabel
    ?humanDescription
    ?pseudonym
    ?place_birth
    ?place_birthLabel
    ?ethnicity
    ?ethnicityLabel
    ?residence
    ?residenceLabel
    ?country_citizen
    ?country_citizenLabel
    ?country_citizenLabelAlt
    ?sex_or_genderLabel
    ?birth_coordinates
    ?birth_country
    ?birth_countryLabel
    ?residence_coordinates
    ?residence_countryLabel

WITH {
SELECT
    ?human
    ?dob
    ?birth_name
    ?given_name
    ?family_name
    ?pseudonym
    ?place_birth
    ?ethnicity
    ?residence
    ?country_citizen
    ?sex_or_gender
    ?birth_coordinates
    ?birth_country
    ?residence_coordinates
    ?residence_country
WHERE {
    ?human wdt:P31 wd:Q5;
    wdt:P27 ?country_citizen, wd:Q145.
  OPTIONAL { ?human wdt:P1477 ?birth_name. }
  OPTIONAL { ?human wdt:P735 ?given_name. }
  OPTIONAL { ?human wdt:P734 ?family_name. }
  OPTIONAL { ?human wdt:P742 ?pseudonym. }
  OPTIONAL { ?human wdt:P569 ?dob. }
  OPTIONAL { ?human wdt:P19 ?place_birth. }
  OPTIONAL { ?human wdt:P172 ?ethnicity. }
  OPTIONAL { ?human wdt:P551 ?residence. }
  OPTIONAL { ?human wdt:P21 ?sex_or_gender. }
  OPTIONAL { ?human wdt:P19 ?place_birth.  ?place_birth wdt:P625  ?birth_coordinates. }
  OPTIONAL { ?human wdt:P19 ?place_birth.  ?place_birth wdt:P17   ?birth_country. }
  OPTIONAL { ?human wdt:P551 ?residence.   ?residence wdt:P625  ?residence_coordinates. }
  OPTIONAL { ?human wdt:P551 ?residence.   ?residence wdt:P17   ?residence_country. }

}
LIMIT 100
} AS %results

WHERE {
  INCLUDE %results.

  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
LIMIT 12345
"""


def get_results(endpoint_url, query):
    user_agent = "WDQS-example Python/%s.%s" % (
        sys.version_info[0],
        sys.version_info[1],
    )
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()


def get_value_from_result(x):
    try:
        return x["value"]
    except:
        return None


def query_with_offset(query, page=0, pagesize=500):
    limit_offset = f" limit {pagesize} offset {page*pagesize}"
    this_query = query.replace("LIMIT 100", limit_offset)
    print(this_query)
    results = get_results(endpoint_url, this_query)
    df = pd.DataFrame(results["results"]["bindings"])
    df = df.applymap(get_value_from_result)
    return df


def replace_url(x):
    try:
        return x.replace("http://www.wikidata.org/entity/", "")
    except:
        return x


def pipe_to_array(x):
    try:
        arr = x.split(" | ")
        return arr
    except:
        return x


def dedupe_and_clean_results(df_to_q):

    tem = "string_agg(distinct {field}, ' | ') as {field}"

    cols = [c for c in list(df_to_q.columns) if c != "human"]

    cols = ", ".join([tem.format(field=c) for c in cols])

    sql = f"""
    select human, {cols}
    from df_to_q
    group by human


    """

    df_deduped = duckdb.query(sql).to_df()

    df_deduped = df_deduped.applymap(replace_url)
    # df_deduped = df_deduped.applymap(pipe_to_array)
    return df_deduped


def get_readable_columns(df):

    df["dob"] = df["dob"].str.slice(0, 10)

    cols = {
        "dob": "dob",
        "humanlabel": "full_name",
        "humanaltlabel": "full_name_alt",
        "birth_name": "full_name_at_birth",
        "humandescription": "person_desc",
        "pseudonym": "pseudonym",
        "country_citizenlabel": "citizenship",
        "sex_or_genderlabel": "gender",
        "birth_countrylabel": "birth_country",
        "ethnicitylabel": "ethnicity",
        "place_birthlabel": "birth_place",
        "birth_coordinates": "birth_place_coordinates",
        "residencelabel": "residence",
        "residence_coordinates": "residence_coordinates",
        "residence_countrylabel": "residence_country",
    }

    cols_to_keep = list(cols.keys())
    df = df[cols_to_keep]
    df = df.rename(columns=cols)

    return df


# VALUES ?lan {{ wd:Q1860}}
#   ?given_name wdt:{id} ?{col_name};
#    wdt:P407 ?lan.
QUERY_NAME_TEMPLATE = """
SELECT ?given_name ?given_nameLabel ?{col_labelled}

WITH {{

SELECT ?given_name  ?{col_name}  {{

  ?given_name (wdt:P31/(wdt:P279*)) wd:Q202444.
  ?given_name wdt:{id} ?{col_name}.



}}
LIMIT 100

}} as %results

WHERE {{
  INCLUDE %results.
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
}}

"""


def get_standardised_name_table(col_name, col_id, page=0, pagesize=5000):
    col_labelled = col_name
    if col_name in ["said_to_be_the_same_as"]:
        col_labelled += "Label"

    query = QUERY_NAME_TEMPLATE.format(
        col_name=col_name, col_labelled=col_labelled, id=col_id
    )

    df = query_with_offset(
        query,
        page,
        pagesize,
    )
    df["name_variant_type"] = col_name
    df = df.rename(
        columns={col_labelled: "alt_name", "given_nameLabel": "original_name"}
    )
    df = df.applymap(replace_url)

    if len(df.columns) == 4:
        return df
    else:
        raise ValueError("df does not contain 4 cols")


# Get all labels associated with person
# SELECT ?wdLabel ?ooLabel
# WHERE {
#   VALUES (?s) {(wd:Q7513)}
#   ?s ?wdt ?o .
#   ?wd wikibase:directClaim ?wdt .
#   ?wd rdfs:label ?wdLabel .
#   OPTIONAL {
#     ?o rdfs:label ?oLabel .
#     FILTER (lang(?oLabel) = "en")
#   }
#   FILTER (lang(?wdLabel) = "en")
#   BIND (COALESCE(?oLabel, ?o) AS ?ooLabel)
#  } ORDER BY xsd:integer(STRAFTER(STR(?wd), "http://www.wikidata.org/entity/P"))


# 465981
