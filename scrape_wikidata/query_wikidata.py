from SPARQLWrapper import SPARQLWrapper, JSON
import sys
import pandas as pd
from scrape_wikidata.cleaning_fns import replace_url

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

# How to get name ordinal
# SELECT
#     ?human
#     ?given_name
#     ?given_nameLabel
#     ?name_num

# WHERE {
#     ?human wdt:P31 wd:Q5.
#     ?human p:P735 ?statement.
#     ?statement ps:P735 ?given_name.
#     OPTIONAL {?statement pq:P1545 ?name_num.}

#     SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }

#   VALUES ?human {wd:Q80}

# }


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


QUERY_CHILDREN = """
SELECT ?human ?child
WHERE
{
  ?human wdt:P31 wd:Q5.
  ?human wdt:P40 ?child.
}
LIMIT 100
"""


QUERY_OCCUPATIONS = """
SELECT
    ?human
    ?occupationLabel

WITH {
SELECT distinct
    ?human
    ?occupation
WHERE {
    ?human wdt:P31 wd:Q5.
    ?human wdt:P106 ?occupation.

}
LIMIT 100
} AS %results

WHERE {
  INCLUDE %results.

  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
LIMIT 123456

"""
