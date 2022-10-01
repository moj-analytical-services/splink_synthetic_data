from functools import partial
import random


def prob_to_bayes_factor(prob):
    return prob / (1 - prob)


def bayes_factor_to_prob(bf):
    return bf / (1 + bf)


def name_inversion(rec, col1, col2):
    rec[col1], rec[col2] = rec[col2], rec[col1]
    return rec


def initial(rec, col):
    rec[col] = rec[col][:1]
    return rec


class CompositeCorruption:
    def __init__(self, baseline_probability=0.1):
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
        if random.uniform(0, 1) < self.adjusted_probability:
            self.reset_probability()
            return self.functions
        else:
            self.reset_probability()
            return []


name_inversion_corruption = CompositeCorruption(baseline_probability=0.9)
name_inversion_corruption.add_corruption_function(
    name_inversion, args={"col1": "first_name", "col2": "surname"}
)


initital_corruption = CompositeCorruption(baseline_probability=0.9)
initital_corruption.add_corruption_function(initial, args={"col": "first_name"})


class ProbabilityAdjustmentFromLookup:
    # Uses a record
    def __init__(self, lookup):
        self.adjustment_lookup = lookup

    def get_adjustment_tuples(self, record):
        adjustment_tuples = []
        for record_column, lookup in self.adjustment_lookup.items():
            record_value = record[record_column]
            adjustment_tuples.extend(lookup[record_value])
        return adjustment_tuples

    def apply_adjustment_weights(self):
        corruption_weight_lookup = self.get_adjustment_weights()
        for corruption, adjustment_weight in corruption_weight_lookup:
            corruption.adjust_probability_using_bayes_factor(adjustment_weight)


class RecordCorruptor:
    def __init__(self):
        self.corruptions = []
        self.record = None
        self.probability_adjustments = []

    def add_corruption_type(self, corruption_type):
        self.corruptions.append(corruption_type)

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

    def apply_corruptions_to_record(self, record):

        functions_to_apply = self.choose_functions_to_apply()
        for f in functions_to_apply:
            record = f(record)

        return record


rc = RecordCorruptor()
rc.add_corruption_type(name_inversion_corruption)
rc.add_corruption_type(initital_corruption)

corruption_lookup = {
    "ethnicity": {
        "white": [(name_inversion_corruption, 10.0)],
        "asian": [(name_inversion_corruption, 2.0)],
    },
    "first_name": {"robin": [(initital_corruption, 10.0)]},
}

adjustment = ProbabilityAdjustmentFromLookup(corruption_lookup)

rc.add_probability_adjustment(adjustment)

record = {"first_name": "robin", "surname": "linacre", "ethnicity": "white"}

rc.apply_probability_adjustments(record)
rc.apply_corruptions_to_record(record)


# cs.add_adjustment_using_scenario({"ethnicity": "white"}, {c.name: 0.5})


# records = []

# for rec in records:
#     sc.record = rec
#     sc.choose_functions_to_apply()


# # def generate_error_vectors(config, num_error_vectors_to_generate):
# #     pass


# # def apply_error_vector(error_vector, formatted_master_record, config):
# #     """
# #     Use an error vector to corrupt a record
# #     """
# #     pass
