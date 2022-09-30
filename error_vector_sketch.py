from copy import deepcopy
import random


def name_inversion_use_surname():
    print("inversion: surname put in forename field")


def name_inversion_use_first_name():
    print("inversion: forename put in surname field")


def transliteration_error():
    print("add transliteration_error")


def jan_first_error():
    print("add jan 1st error")


def typo_error():
    print("typo occurs with probability p")


# Composite errors.  i.e. when these error type is picked, errors happen for sure
# Anything left blank in these dictionaries means no error


name_inversion_error = {
    "first_name": [name_inversion_use_surname],
    "surname": [name_inversion_use_first_name],
}

# Note they're lists to  allow for multiple error functions to be applied
# one after the other
name_inversion_with_transliteration_error = {
    "first_name": [name_inversion_use_surname, transliteration_error],
    "surname": [name_inversion_use_first_name, transliteration_error],
}


transliteration_error = {
    "first_name": [transliteration_error],
    "surname": [transliteration_error],
}

# Non composite errors
dob_jan_error = {
    "dob": [jan_first_error],
}


# Would need to modify typo error so it's aware of which field to apply the typo to
typo_first_name = {
    "first_name": [typo_error],
}

typo_surname = {
    "surname": [typo_error],
}

# What are the baseline probabilities for these errors occurring?
composite_error_types_with_baseline_probabilities = {
    "name_inversion_error": {
        "definition": name_inversion_error,
        "baseline_probability": 0.05,
    },
    "name_inversion_with_transliteration_error": {
        "definition": name_inversion_with_transliteration_error,
        "baseline_probability": 0.05,
    },
    "transliteration_error": {
        "definition": transliteration_error,
        "baseline_probability": 0.05,
    },
    "dob_jan_error": {"definition": dob_jan_error, "baseline_probability": 0.02},
    "typo_first_name": {"definition": typo_first_name, "baseline_probability": 0.1},
    "typo_surname": {"definition": typo_surname, "baseline_probability": 0.05},
}


# Adjustments to baseline probabilities
# Anything left blank in this table gets baseline probabilities


# The interpretation of the below table is:
# if ethnicity is white then name inversion error becomes half as likely,
# transliteration error becomes a fifth as likely etc.
# if ethnicity is not white or asian, make no adjustment to baseline_probability

ethnicity_adjustments = {
    "white": {
        "name_inversion_error": 0.5,
        "name_inversion_with_transliteration_error": 0.2,
        "transliteration_error": 0.2,
    },
    "asian": {
        "name_inversion_error": 3.0,
        "name_inversion_with_transliteration_error": 3.0,
        "transliteration_error": 3.0,
    },
}


country_of_birth_adjustments = {"UK": {"dob_jan_error": 0.01}}

# Adjustment lookup:

probability_adjustments = {
    "ethnicity": {
        "white": {
            "name_inversion_error": 0.5,
            "name_inversion_with_transliteration_error": 0.2,
            "transliteration_error": 0.2,
        },
        "asian": {
            "name_inversion_error": 3.0,
            "name_inversion_with_transliteration_error": 3.0,
            "transliteration_error": 3.0,
        },
    },
    "country_of_birth": {"UK": {"dob_jan_error": 0.01}},
}

# Now generate the error vector

record = {
    "ethnicity": "white",
    "country_of_birth": "UK",
    "first_name": "john",
    "surname": "smith",
    "dob": "1975-03-28",
    "postcode": "AB1 2CD",
}


def prob_to_bayes_factor(prob):
    return prob / (1 - prob)


def bayes_factor_to_prob(bf):
    return bf / (1 + bf)


def adjustment_probability_using_bayes_factor(prob, bf_adjustment):
    bf = prob_to_bayes_factor(prob)
    bf = bf * bf_adjustment
    return bayes_factor_to_prob(bf)


# Adjust the probabilities of the various error types occuring according to the
# probability_adjustments lookup
this_record_probabilities = deepcopy(composite_error_types_with_baseline_probabilities)
for error_type_str, d in this_record_probabilities.items():
    definition = d["definition"]
    prob = d["baseline_probability"]
    d["adjusted_probability"] = prob

    # Make probability adjustments
    for column_name, adjustments in probability_adjustments.items():
        record_value = record[column_name]
        if record_value in adjustments:
            if error_type_str in adjustments[record_value]:
                adjustment = adjustments[record_value][error_type_str]
                prob = adjustment_probability_using_bayes_factor(prob, adjustment)
    d["adjusted_probability"] = prob

# Apply the this_record_probabiilties
for error_type_str, d in this_record_probabilities.items():
    definition = d["definition"]
    probability_error_is_applied = d["adjusted_probability"]

    # TODO: probably want to dedupe list of error functions
    # so each one is applied at most once.

    # if random.uniform(0, 1) < probability_error_is_applied:

    # this is just for debugging so that each error is called with 100% probability
    # you really want the line above
    if True:
        for column_name, error_fn_list in definition.items():
            for error_fn in error_fn_list:
                error_fn()
