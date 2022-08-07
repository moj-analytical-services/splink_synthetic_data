# Running these scripts

## Setup

Install poetry, which we will use for dependency management, using the insructions on their homepage [here](https://python-poetry.org/docs/#installation)

Use poetry to install the dependencies. The first line is optional but I find it useful to have the `.venv` folder in the project directory.

```
poetry config virtualenvs.in-project true
poetry install
```

In VS code, ensure the selected Python interpreter corresponds to the venv using command pallette -> "Python: Select Interpreter"

## Scraping data from wikidata

Wikidata provides a query service at https://query.wikidata.org/

There are limits to the number of records returned by the service so you must paginate results (e.g. request the first 5,000 records, then 5,001 to 10,000, etc.)

In the scripts you'll see a loop like:

```
for page in range(1, 100):
```

Results are saved out to make it clear how far the scrape has progressed e.g. `page_1_5000_to_9999.parquet`

This allows you to re-start a partially completed scrape. So if you see you've completed pages 1 to 25, you could alter that to

```
for page in range(26, 100):
```

to restart at page 26.

There's also a rate limiter `time.sleep(45)` so we don't spam the wikidata service with too many requests.

## Scraping persons

You can scrape entities of the type human (`?human wdt:P31 wd:Q5.`) using

`python 01_scrape_persons.py`.

This is a long running operation and results are paginated. You can re-start from a partially completed scrape by changing the start of the range.

---

Plan:

- Scrape
