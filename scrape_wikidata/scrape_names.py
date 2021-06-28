# SELECT ?given_name ?given_nameLabel ?short_name ?transliteration  ?said_to_be_the_same_asLabel ?nickname WHERE {
#   ?given_name (wdt:P31/(wdt:P279*)) wd:Q202444.
#   VALUES ?given_name { wd:Q4927524}


#   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
#   OPTIONAL { ?given_name wdt:P1813 ?short_name. }
#   OPTIONAL { ?given_name wdt:P2440 ?transliteration. }
#   OPTIONAL { ?given_name wdt:P460 ?said_to_be_the_same_as. }
#   OPTIONAL { ?given_name wdt:P1449 ?nickname. }
# }
# LIMIT 10


QUERY_SHORT_NAME = """
SELECT ?given_name ?given_nameLabel ?short_name  {
  ?given_name (wdt:P31/(wdt:P279*)) wd:Q202444.
  VALUES ?given_name { wd:Q4927524}
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  OPTIONAL { ?given_name wdt:P1813 ?short_name. }


}
LIMIT 100
"""
