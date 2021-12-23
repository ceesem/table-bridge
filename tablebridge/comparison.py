import pandas as pd
import numpy as np
from .utils import row_differs


def fill_column_from_above(df, column, starting_value=None, inplace=False):
    """Fill in a column in order with the last non-NAN value

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to act on
    column : str
        Column name
    starting_value : object
        Initial value to use if there is none.
    inplace : bool, optional
        If True, make changes in place, by default False

    Returns
    -------
    df
        Dataframe with new data
    """
    if inplace:
        df = df.copy()

    last_value = df[column].iloc[0]
    if pd.isna(last_value):
        if not starting_value:
            last_value = pd.NA
        else:
            last_value = starting_value

    for idx, val in df[column].iteritems():
        if pd.isna(val):
            df.at[idx, column] = last_value
        else:
            last_value = val
    return df


def convert_dataframe(df, schema):
    """Convert dataframe via column renaming or functions that map rows to values.

    Parameters
    ----------
    df : pd.DataFrame
        Original DataFrame for conversion
    schema : dict
        dict whose keys are strings and whose values are either strings or functions.
        The converted dataframe will have columns based on the keys of the dict.
        Strings are interpreted as a simple renaming from the value (in the original column names)
        to the key (in the returned data).
        Functions are passed to df.apply(func, axis=1) and should be defined accordingly.
        Only those columns with keys passed will be returned, even if no alterations are needed.

    Returns
    -------
    dataframe
        Converted dataframe with columns specified in schema.
    """
    rename_schema = {}
    apply_schema = {}
    cols_out = []

    df = df.copy()
    for k, v in schema.items():
        if isinstance(v, str):
            rename_schema[v] = k
            cols_out.append(k)
        else:
            apply_schema[k] = v
            cols_out.append(k)
    for column, generator in apply_schema.items():
        df[column] = df.apply(generator, axis=1)
    df = df.rename(columns=rename_schema)
    return df[cols_out]


class AnnotationComparison:
    def __init__(self, new_df, old_df, id_column):
        self._new_df = new_df
        self._old_df = old_df
        self._outer_merged_df = None
        self._common_merged_df = None
        self._id_column = id_column
        if len(self.data_columns) == 0:
            raise ValueError(
                "DataFrames must have at least one data column beyond the index"
            )

    @property
    def new_df(self):
        return self._new_df

    @property
    def old_df(self):
        return self._old_df

    @property
    def data_columns(self):
        data_col = list(self.new_df.columns)
        data_col.remove(self.id_column)
        return data_col

    @property
    def id_column(self):
        return self._id_column

    @property
    def data_columns_new(self):
        return [f"{x}_new" for x in self.data_columns]

    @property
    def data_columns_old(self):
        return [f"{x}_old" for x in self.data_columns]

    @property
    def outer_merged_df(self):
        if self._outer_merged_df is None:
            self._outer_merged_df = self._compute_merged_df("outer")
        return self._outer_merged_df

    @property
    def common_merged_df(self):
        if self._common_merged_df is None:
            self._common_merged_df = self._compute_merged_df("inner")
        return self._common_merged_df

    def _compute_merged_df(self, how="inner"):
        return self._new_df.merge(
            self._old_df,
            on=self._id_column,
            how=how,
            suffixes=("_new", "_old"),
            validate="1:1",
        )

    @property
    def new_id_column(self):
        return f"{self._index_column_new}_new"

    @property
    def old_id_column(self):
        return f"{self._index_column_new}_old"

    def new_annotations(self):
        not_in_old = ~np.isin(self.new_df[self.id_column], self.old_df[self.id_column])
        return self.new_df[not_in_old]

    def removed_annotations(self):
        not_in_new = ~np.isin(self.old_df[self.id_column], self.new_df[self.id_column])
        return self.old_df[not_in_new]

    def changed_annotations(self):
        row_is_diff = self.common_merged_df.apply(
            lambda x: row_differs(x, self.data_columns_new, self.data_columns_old),
            axis=1,
        )
        common_ids = self.common_merged_df[row_is_diff][self.id_column]
        return self.new_df.query(f"{self.id_column} in @common_ids")

    def unchanged_annotations(self):
        changed_df = self.changed_annotations()[self.id_column]
        removed_df = self.removed_annotations()[self.id_column]
        altered_ids = np.concatenate((changed_df, removed_df))

        all_ids = self.old_df[self.id_column].values
        unchanged_ids = all_ids[~np.isin(all_ids, altered_ids)]

        return self.old_df.query(f"{self.id_column} in @unchanged_ids")
