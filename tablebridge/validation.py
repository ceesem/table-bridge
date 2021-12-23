import numpy as np
import pandas as pd
import re


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