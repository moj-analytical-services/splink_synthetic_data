from functools import partial
import random
import logging
import pandas as pd
import duckdb


logger = logging.getLogger(__name__)


def prob_to_bayes_factor(prob):
    return prob / (1 - prob)


def bayes_factor_to_prob(bf):
    return bf / (1 + bf)


class CompositeCorruption:
    def __init__(self, name="", baseline_probability=0.1):
        self.name = name
        self.functions = []
        self.baseline_probability = baseline_probability
        self.adjusted_probability = None

    def add_corruption_function(self, fn, args):
        curried = partial(fn, **args)
        self.functions.append(curried)

    def reset_probability(self):
        self.adjusted_probability = self.baseline_probability

    def adjust_probability_using_bayes_factor(self, bayes_factor_adjustment):
        bf = prob_to_bayes_factor(self.adjusted_probability)
        bf = bf * bayes_factor_adjustment
        self.adjusted_probability = bayes_factor_to_prob(bf)

    def sample(self):
        """activate corruption functions based on self.adjusted_probability"""

        logger.debug(
            f"Probability {self.name} composite corruption will be selected is "
            f"{self.adjusted_probability}"
        )

        if random.uniform(0, 1) < self.adjusted_probability:
            self.reset_probability()
            return self.functions
        else:
            self.reset_probability()
            return []


class ProbabilityAdjustmentFromLookup:
    def __init__(self, lookup):
        """
        lookup is a lookup between fields in the input record, their values, and
        the adjustments to make to the probability of activation, specified as
        a bayes factor

        e.g.
        {
            "ethnicity": {
                "white": [(name_inversion_corruption, 0.1)],
                "asian": [(name_inversion_corruption, 5.0)],
            },
            "first_name": {"robin": [(initital_corruption, 2.0)]},
        }

        """
        self.adjustment_lookup = lookup

    def get_adjustment_tuples(self, record):
        """
        Uses the record to lookup and return a list of adjustment tuples like:
        [
            (name_inversion_corruption, 0.1),
            (initital_corruption, 2.0)
        ]
        i.e.
            - decrease the probability of activating the name inversion corruption
                using a bayes factor of 0.1
            - increase the probability of activating the initial_corruption
                using a bayes factor of 2.0
        """
        adjustment_tuples = []
        for record_column, lookup in self.adjustment_lookup.items():
            record_value = record[record_column]

            if record_value in lookup:
                logger.debug(
                    f"Record column: {record_column} "
                    f"Record value: {record_value}, selected adjustment tuple "
                    f"{lookup[record_value]}"
                )
                adjustment_tuples.extend(lookup[record_value])

        return adjustment_tuples


class ProbabilityAdjustmentFromSQL:
    def __init__(self, sql, composite_corruption: CompositeCorruption, bayes_factor):
        """
        sql is a sql expression like:
        'len(first_name) < 3 and len(surname) < 3'

        }

        """
        self.sql = sql
        self.composite_corruption = composite_corruption
        self.bayes_factor = bayes_factor

    def get_adjustment_tuples(self, record):
        """
        Uses the record to lookup and return a list of adjustment tuples like:
        [
            (name_inversion_corruption, 0.1),
            (initital_corruption, 2.0)
        ]
        i.e.
            - decrease the probability of activating the name inversion corruption
                using a bayes factor of 0.1
            - increase the probability of activating the initial_corruption
                using a bayes factor of 2.0
        """
        df = pd.DataFrame([record])
        con = duckdb.connect()
        con.register("df", df)
        sql = f"""
        select {self.sql} as condition_matches
        from df
        """
        matches = con.execute(sql).fetchone()[0]

        if matches:
            adjustment_tuples = [(self.composite_corruption, self.bayes_factor)]
            logger.debug(
                f"SQL condition {self.sql} matched, "
                f" tuples selected: {adjustment_tuples}"
            )
            return adjustment_tuples
        else:
            return []


class RecordCorruptor:
    def __init__(self):
        self.corruptions: list[CompositeCorruption] = []
        self.probability_adjustments = []

    def add_composite_corruption(self, composite_corruption: CompositeCorruption):
        self.corruptions.append(composite_corruption)

    def add_probability_adjustment(self, adjustment):
        self.probability_adjustments.append(adjustment)

    def probability_adjustment_tuples(self, record):
        tuples = []
        for pa in self.probability_adjustments:
            new_tuples = pa.get_adjustment_tuples(record)
            tuples.extend(new_tuples)

        return tuples

    def apply_probability_adjustments(self, record):
        for c in self.corruptions:
            c.reset_probability()
        for corruption, bayes_factor in self.probability_adjustment_tuples(record):
            corruption.adjust_probability_using_bayes_factor(bayes_factor)

    def choose_functions_to_apply(self):
        functions = []
        for c in self.corruptions:
            functions.extend(c.sample())
        return functions

    def apply_corruptions_to_record(self, formatted_master_data, record_to_modify):
        functions_to_apply = self.choose_functions_to_apply()
        for f in functions_to_apply:
            record_to_modify = f(
                formatted_master_data,
                record_to_modify,
            )
            record_to_modify["corruptions_applied"].append(f.func.__name__)

        return record_to_modify
