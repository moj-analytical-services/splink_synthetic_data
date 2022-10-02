import math
import random
from numpy.random import chisquare


def offset_by_distance_in_random_direction(geo_struct, distance_km):

    lat = geo_struct["lat"]
    lng = geo_struct["lng"]

    radians = random.uniform(0, 2 * math.pi)

    dx = math.sin(radians) * distance_km
    dy = math.cos(radians) * distance_km

    r_earth = 6371  # Radius of earth in kilometers.

    new_lat = lat + (dy / r_earth) * (180 / math.pi)
    new_lng = lng + (dx / r_earth) * (180 / math.pi) / math.cos(lat * math.pi / 180)

    return {"lat": new_lat, "lng": new_lng}


def lat_lng_corrupt_distance(
    formatted_master_record,
    record_to_modify,
    input_colname,
    output_colname,
    distance_min,
    distance_max,
):

    if not formatted_master_record[input_colname]:
        record_to_modify[output_colname] = None
        return record_to_modify
    else:
        geostruct = formatted_master_record[input_colname][0]
        # Chisquare 3 runs between 0 and about 10
        chi = chisquare(3, 1)
        multiplier = (distance_max - distance_min) / 10
        distance = (float(chi[0]) * multiplier) + distance_min
        new_geostruct = offset_by_distance_in_random_direction(geostruct, distance)
        record_to_modify[output_colname] = new_geostruct
    return record_to_modify


def lat_lng_uncorrupted_record(
    formatted_master_record, input_colname, output_colname, record_to_modify={}
):
    if not formatted_master_record[input_colname]:
        record_to_modify[output_colname] = None
    else:
        record_to_modify[output_colname] = formatted_master_record[input_colname][0]
    return record_to_modify
