# This file contains excerpts copied from corrupter.py, and basefunctions.py
# translated to Python 3
# The original library can be found at https://dmm.anu.edu.au/geco/
#
# The original file header is as follows
#
# corrupter.py - Python module to corrupt (modify) generate synthetic data.
#
#                Part of a flexible data generation system.
#
# Peter Christen and Dinusha Vatsalan,  January-March 2012
# =============================================================================
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# =============================================================================

import random
import types
import numpy as np


def check_is_non_empty_string(variable, value):
    """Check if the value given is of type string and is not an empty string.

    The argument 'variable' needs to be set to the name (as a string) of the
    value which is checked.
    """

    if (not isinstance(variable, str)) or (variable == ""):
        raise Exception(
            'Value of "variable" is not a non-empty string: %s (%s)'
            % (str(variable), type(variable))
        )

    if (not isinstance(value, str)) or (value == ""):
        raise Exception(
            'Value of "%s" is not a non-empty string: %s (%s)'
            % (variable, str(value), type(value))
        )


def check_is_normalised(variable, value):
    """Check if the value given is a number, i.e. of type integer or float, and
    between (including) 0.0 and 1.0.

    The argument 'variable' needs to be set to the name (as a string) of the
    value which is checked.
    """

    check_is_non_empty_string("variable", variable)

    if (
        (not isinstance(value, int))
        and (not isinstance(value, float))
        or (value < 0.0)
        or (value > 1.0)
    ):
        raise Exception(
            'Value of "%s" is not a normalised number ' % (variable)
            + "(between 0.0 and 1.0): %s (%s)" % (str(value), type(value))
        )


def position_mod_uniform(in_str):
    """Select any position in the given input string with uniform likelihood.

    Return 0 is the string is empty.
    """

    if in_str == "":  # Empty input string
        return 0

    max_pos = len(in_str) - 1

    pos = random.randint(0, max_pos)  # String positions start at 0

    return pos


def check_is_function_or_method(variable, value):
    """Check if the value given is a function or method.

    The argument 'variable' needs to be set to the name (as a string) of the
    value which is checked.
    """

    check_is_non_empty_string("variable", variable)

    if type(value) not in [types.FunctionType, types.MethodType]:
        raise Exception(
            "%s is not a function or method: %s" % (str(variable), type(value))
        )


class CorruptValue:
    """Base class for the definition of corruptor that is applied on a single
    attribute (field) in the data set.

    This class and all of its derived classes provide methods that allow the
    definition of how values in a single attribute are corrupted (modified)
    and the parameters necessary for the corruption process.

    The following variables need to be set when a CorruptValue instance is
    initialised (with further parameters listed in the derived classes):

    position_function  A function that (somehow) determines the location
                       within a string value of where a modification
                       (corruption) is to be applied. The input of this
                       function is assumed to be a string and its return value
                       an integer number in the range of the length of the
                       given input string.
    """

    # ---------------------------------------------------------------------------

    def __init__(self, base_kwargs):
        """Constructor, set general attributes."""

        # General attributes for all attribute corruptors.
        #
        self.position_function = None

        # Process the keyword argument (all keywords specific to a certain data
        # generator type were processed in the derived class constructor)
        #
        for (keyword, value) in list(base_kwargs.items()):

            if keyword.startswith("position"):
                check_is_function_or_method("position_function", value)
                self.position_function = value

            else:
                raise Exception(
                    'Illegal constructor argument keyword: "%s"' % (str(keyword))
                )

        check_is_function_or_method("position_function", self.position_function)

        # Check if the position function does return an integer value
        #
        pos = self.position_function("test")
        if (not isinstance(pos, int)) or (pos < 0) or (pos > 3):
            raise Exception(
                "Position function returns an illegal value (either"
                + "not an integer or and integer out of range: %s" % (str(pos))
            )

    # ---------------------------------------------------------------------------

    def corrupt_value(self, str):
        """Method which corrupts the given input string and returns the modified
        string.
        See implementations in derived classes for details.
        """

        raise Exception("Override abstract method in derived class")


class CorruptValueQuerty(CorruptValue):
    """Use a keyboard layout to simulate typing errors. They keyboard is
    hard-coded into this method, but can be changed easily for different
    keyboard layout.

    A character from the original input string will be randomly chosen using
    the position function, and then a character from either the same row or
    column in the keyboard will be selected.

    The additional arguments (besides the base class argument
    'position_function') that have to be set when this attribute type is
    initialised are:

    row_prob  The probability that a neighbouring character in the same row
              is selected.

    col_prob  The probability that a neighbouring character in the same
              column is selected.

    The sum of row_prob and col_prob must be 1.0.
    """

    # ---------------------------------------------------------------------------

    def __init__(self, **kwargs):
        """Constructor. Process the derived keywords first, then call the base
        class constructor.
        """

        self.row_prob = None
        self.col_prob = None
        self.name = "Keybord value"

        # Process all keyword arguments
        #
        base_kwargs = {}  # Dictionary, will contain unprocessed arguments

        for (keyword, value) in list(kwargs.items()):

            if keyword.startswith("row"):
                check_is_normalised("row_prob", value)
                self.row_prob = value

            elif keyword.startswith("col"):
                check_is_normalised("col_prob", value)
                self.col_prob = value

            else:
                base_kwargs[keyword] = value

        CorruptValue.__init__(self, base_kwargs)  # Process base arguments

        # Check if the necessary variables have been set
        #
        check_is_normalised("row_prob", self.row_prob)
        check_is_normalised("col_prob", self.col_prob)

        if abs((self.row_prob + self.col_prob) - 1.0) > 0.0000001:
            raise Exception(
                "Sum of row and column probablities does not sum " + "to 1.0"
            )

        # Keyboard substitutions gives two dictionaries with the neigbouring keys
        # for all leters both for rows and columns (based on ideas implemented by
        # Mauricio A. Hernandez in his dbgen).
        # This following data structures assume a QWERTY keyboard layout
        #
        self.rows = {
            "a": "s",
            "b": "vn",
            "c": "xv",
            "d": "sf",
            "e": "wr",
            "f": "dg",
            "g": "fh",
            "h": "gj",
            "i": "uo",
            "j": "hk",
            "k": "jl",
            "l": "k",
            "m": "n",
            "n": "bm",
            "o": "ip",
            "p": "o",
            "q": "w",
            "r": "et",
            "s": "ad",
            "t": "ry",
            "u": "yi",
            "v": "cb",
            "w": "qe",
            "x": "zc",
            "y": "tu",
            "z": "x",
            "1": "2",
            "2": "13",
            "3": "24",
            "4": "35",
            "5": "46",
            "6": "57",
            "7": "68",
            "8": "79",
            "9": "80",
            "0": "9",
        }

        self.cols = {
            "a": "qzw",
            "b": "gh",
            "c": "df",
            "d": "erc",
            "e": "ds34",
            "f": "rvc",
            "g": "tbv",
            "h": "ybn",
            "i": "k89",
            "j": "umn",
            "k": "im",
            "l": "o",
            "m": "jk",
            "n": "hj",
            "o": "l90",
            "p": "0",
            "q": "a12",
            "r": "f45",
            "s": "wxz",
            "t": "g56",
            "u": "j78",
            "v": "fg",
            "w": "s23",
            "x": "sd",
            "y": "h67",
            "z": "as",
            "1": "q",
            "2": "qw",
            "3": "we",
            "4": "er",
            "5": "rt",
            "6": "ty",
            "7": "yu",
            "8": "ui",
            "9": "io",
            "0": "op",
        }

    # ---------------------------------------------------------------------------

    def corrupt_value(self, in_str):
        """Method which corrupts the given input string by replacing a single
        character with a neighbouring character given the defined keyboard
        layout at a position randomly selected by the position function.
        """

        if len(in_str) == 0:  # Empty string, no modification possible
            return in_str

        max_try = 10  # Maximum number of tries to find a keyboard modification at
        # a randomly selected position

        done_key_mod = False  # A flag, set to true once a modification is done
        try_num = 0

        mod_str = in_str[:]  # Make a copy of the string which will be modified

        while (done_key_mod == False) and (try_num < max_try):

            mod_pos = self.position_function(mod_str)
            mod_char = mod_str[mod_pos]

            r = random.random()  # Create a random number between 0 and 1

            if r <= self.row_prob:  # See if there is a row modification
                if mod_char in self.rows:
                    key_mod_chars = self.rows[mod_char]
                    done_key_mod = True

            else:  # See if there is a column modification
                if mod_char in self.cols:
                    key_mod_chars = self.cols[mod_char]
                    done_key_mod = True

            if done_key_mod == False:
                try_num += 1

        # If a modification is possible do it
        #
        if done_key_mod == True:

            # Randomly select one of the possible characters
            #
            new_char = random.choice(key_mod_chars)

            mod_str = mod_str[:mod_pos] + new_char + mod_str[mod_pos + 1 :]

        assert len(mod_str) == len(in_str)

        return mod_str


class CorruptValueNumpad(CorruptValue):
    """Use a keyboard layout to simulate typing errors. They keyboard is
    hard-coded into this method, but can be changed easily for different
    keyboard layout.

    A character from the original input string will be randomly chosen using
    the position function, and then a character from either the same row or
    column in the keyboard will be selected.

    The additional arguments (besides the base class argument
    'position_function') that have to be set when this attribute type is
    initialised are:

    row_prob  The probability that a neighbouring character in the same row
              is selected.

    col_prob  The probability that a neighbouring character in the same
              column is selected.

    The sum of row_prob and col_prob must be 1.0.
    """

    # ---------------------------------------------------------------------------

    def __init__(self, **kwargs):
        """Constructor. Process the derived keywords first, then call the base
        class constructor.
        """

        self.row_prob = None
        self.col_prob = None
        self.name = "Keybord value"

        # Process all keyword arguments
        #
        base_kwargs = {}  # Dictionary, will contain unprocessed arguments

        for (keyword, value) in list(kwargs.items()):

            if keyword.startswith("row"):
                check_is_normalised("row_prob", value)
                self.row_prob = value

            elif keyword.startswith("col"):
                check_is_normalised("col_prob", value)
                self.col_prob = value

            else:
                base_kwargs[keyword] = value

        CorruptValue.__init__(self, base_kwargs)  # Process base arguments

        # Check if the necessary variables have been set
        #
        check_is_normalised("row_prob", self.row_prob)
        check_is_normalised("col_prob", self.col_prob)

        if abs((self.row_prob + self.col_prob) - 1.0) > 0.0000001:
            raise Exception(
                "Sum of row and column probablities does not sum " + "to 1.0"
            )

        # Keyboard substitutions gives two dictionaries with the neigbouring keys
        # for all leters both for rows and columns (based on ideas implemented by
        # Mauricio A. Hernandez in his dbgen).
        # This following data structures assume a QWERTY keyboard layout
        #
        self.rows = {
            "0": "08",
            "1": "127",
            "2": "135",
            "3": "28",
            "4": "5",
            "5": "462",
            "6": "58",
            "7": "18",
            "8": "796",
            "9": "8",
        }

        self.cols = {
            "0": "128",
            "1": "407",
            "2": "505",
            "3": "86",
            "4": "17",
            "5": "822",
            "6": "938",
            "7": "14",
            "8": "569",
            "9": "68",
        }

    # ---------------------------------------------------------------------------

    def corrupt_value(self, in_str):
        """Method which corrupts the given input string by replacing a single
        character with a neighbouring character given the defined keyboard
        layout at a position randomly selected by the position function.
        """

        if len(in_str) == 0:  # Empty string, no modification possible
            return in_str

        max_try = 10  # Maximum number of tries to find a keyboard modification at
        # a randomly selected position

        done_key_mod = False  # A flag, set to true once a modification is done
        try_num = 0

        mod_str = in_str[:]  # Make a copy of the string which will be modified

        while (done_key_mod == False) and (try_num < max_try):

            mod_pos = self.position_function(mod_str)
            mod_char = mod_str[mod_pos]

            r = random.random()  # Create a random number between 0 and 1

            if r <= self.row_prob:  # See if there is a row modification
                if mod_char in self.rows:
                    key_mod_chars = self.rows[mod_char]
                    done_key_mod = True

            else:  # See if there is a column modification
                if mod_char in self.cols:
                    key_mod_chars = self.cols[mod_char]
                    done_key_mod = True

            if done_key_mod == False:
                try_num += 1

        # If a modification is possible do it
        #
        if done_key_mod == True:

            # Randomly select one of the possible characters
            #
            new_char = random.choice(key_mod_chars)

            mod_str = mod_str[:mod_pos] + new_char + mod_str[mod_pos + 1 :]

        assert len(mod_str) == len(in_str)

        return mod_str


def get_zipf_dist(max_num_dup_per_rec):

    num_dup = 1
    number_of_org_records = 1

    zipf_theta = 0.5
    prob_sum = 0.0
    denom = 0.0

    prob_dist_list = [(num_dup, prob_sum)]

    for i in range(number_of_org_records):
        denom += 1.0 / (i + 1) ** (1.0 - zipf_theta)

    zipf_c = 1.0 / denom
    zipf_num = []  # A list of Zipf numbers
    zipf_sum = 0.0  # The sum of all Zipf number

    for i in range(max_num_dup_per_rec):
        zipf_num.append(zipf_c / ((i + 1) ** (1.0 - zipf_theta)))
        zipf_sum += zipf_num[-1]

    for i in range(max_num_dup_per_rec):  # Scale so they sum up to 1.0
        zipf_num[i] = zipf_num[i] / zipf_sum

    for i in range(max_num_dup_per_rec - 1):
        num_dup += 1
        prob_dist_list.append((num_dup, zipf_num[i]))

    vals, weights = list(zip(*prob_dist_list))
    weights = [w / sum(weights) for w in weights]
    return {"vals": vals, "weights": weights}
