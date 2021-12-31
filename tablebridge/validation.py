import numpy as np
import pandas as pd
import re
from collections import abc

__all__ = ["process_int", "process_uint64", "process_point", "no_validation"]


def process_int(x):
    try:
        return int(x)
    except:
        return pd.NA


def process_uint64(x):
    try:
        return np.uint64(x)
    except:
        return pd.NA


def process_point(x):
    if len(x) == 0:
        return pd.NA
    grp = re.search("(\d+)[,\s]*(\d+)[,\s]*(\d+)", x)
    if grp is None:
        return pd.NA
    else:
        return [int(x) for x in grp.groups()]


def no_validation(x):
    if len(x) == 0 or pd.isna(x):
        return pd.NA
    else:
        return x


def stringify_list(x):
    if np.any(pd.isna(x)):
        return ""
    else:
        try:
            return ", ".join([str(i) for i in x])
        except:
            return str(x)


def blankify_nan(x):
    if np.any(pd.isna(x)):
        return ""
    else:
        return str(x)


def is_listlike(x):
    excluded_types = str
    return isinstance(x, abc.Iterable) and not isinstance(x, excluded_types)


def process_column(col):
    return [stringify_list(x) if is_listlike(x) else blankify_nan(x) for x in col]
