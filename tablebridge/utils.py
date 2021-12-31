import numpy as np
import pandas as pd
import string


def row_differs(row, data_columns_a, data_columns_b):
    for dca, dcb in zip(data_columns_a, data_columns_b):
        if np.all(pd.isna(row[[dca, dcb]])):
            continue
        if np.any(row[dca] != row[dcb]):
            return True
    return False


def number_to_column(n, offset=0):
    n = n + offset
    letters = ""
    divisor = 1
    while divisor > 0:
        letters = string.ascii_uppercase[np.remainder(n, 25)] + letters
        divisor = np.floor(n / 25).astype(int)
        n = divisor - 1
    return letters


def column_to_number(aa):
    number = 0
    for ii, letter in enumerate(aa[::-1]):
        base = 25 ** ii
        dig = (
            np.flatnonzero(np.array([x for x in string.ascii_uppercase]) == letter)[0]
            + 1
        )
        number += dig * base
    return number - 1
