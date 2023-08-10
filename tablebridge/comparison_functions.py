import pandas as pd
import itertools


def nanf(*args):
    "Returns a nan in every row, equivalent to constant(pd.NA). Note this is already a function, not a factory!"
    return pd.NA


def constant(value):
    "Returns a function that returns a constant for every row"

    def func(*args):
        return value

    return func


def count_from(start, step=1, prefix=None):
    """Factory to build a counter starting at a given value, with optional step size and prefix.

    Parameters
    ----------
    start : int
        Number at which to start counting.
    step : int, optional
        Count step size, by default 1
    prefix : Str or None, optional
        If set, sets the prefix before the output number, by default None

    Returns
    -------
    function
        Function that returns the next element in the list described above.
    """
    counter = itertools.count(start, step=step)

    def func(*args):
        if prefix:
            return f"{prefix}{next(counter)}"
        else:
            return next(counter)

    return func
