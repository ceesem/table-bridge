import re
import pandas as pd
import numpy as np
import os
from .utils import number_to_column, column_to_number
from .auth import get_credentials, HttpError
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]  # read+write scope

__all__ = ["SheetProcessor"]


class SheetProcessor:
    def __init__(
        self,
        sheet_id,
        token_file,
        column_names,
        first_row=0,
        first_column="A",
        sheet_name=None,
        validation_map=None,
        secrets_file=None,
    ):
        self._service = sheet_service(token_file, secrets_file)
        if validation_map is None:
            validation_map = {}
        self._validation_map = validation_map
        self._sheet_name = sheet_name
        self._sheet_id = sheet_id
        self._column_names = column_names
        self._column_mapping = None
        self._first_row = first_row
        self._first_column = first_column
        self._data = None

    @property
    def sheet_name(self):
        return self._sheet_name

    @property
    def column_names(self):
        return self._column_names

    @property
    def column_mapping(self):
        if self._column_mapping is None:
            col_offset = column_to_number(self._first_column)
            self._column_mapping = {
                cn: number_to_column(ii, col_offset)
                for ii, cn in enumerate(self._column_names)
            }
        return self._column_mapping

    @property
    def row_offset(self):
        return self._first_row

    @property
    def data(self):
        if self._data is None:
            self.update_data()
        return self._data

    def update_data(self):
        df = self._get_data()
        row_index = self.row_mapping(df.index.values)
        df.index = pd.Index(row_index, name="table_row", dtype=int)
        self._data = df
        return self.data

    def row_mapping(self, row_inds):
        return np.array(row_inds) + self.row_offset

    def _get_data(self):
        last_column = self.column_mapping[self.column_names[-1]]
        return self._get_data_range(
            (self._first_row, None), (self._first_column, last_column)
        )

    @property
    def sheet_range(self):
        last_column = self.column_mapping[self.column_names[-1]]
        return set_range(
            (self._first_row, None), (self._first_column, last_column), self.sheet_name
        )

    def _get_data_range_raw(self, rows, columns, sheet_name=None):
        if sheet_name is None:
            sheet_name = self.sheet_name

        range_str = set_range(rows, columns, sheet_name)
        return get_sheet_raw(self._service, self._sheet_id, range=range_str)

    def _get_data_range(self, rows, columns, sheet_name=None):
        data = self._get_data_range_raw(rows, columns, sheet_name)
        return process_records(data["values"], self.column_names, self._validation_map)

    def set_values(self, rows, columns, values):
        r = update_cells(
            rows, columns, values, self._service, self._sheet_id, self._sheet_name
        )
        return r

    def set_value_series(self, s):
        rows = s.index.values
        columns = [self.column_mapping[s.name]] * len(rows)
        values = s.values
        self.set_values(rows, columns, values)

    def append_data(self, data, add_blank_rows=False):
        """Assume data is same column form as downloaded data"""
        data_ready = data.fillna("").values
        return append_cells(
            data_ready,
            range=self.sheet_range,
            service=self._service,
            sheet_id=self._sheet_id,
            add_blank_rows=add_blank_rows,
        )


def sheet_service(token_file, secrets_file):
    creds = get_credentials(token_file, secrets_file)
    service = build("sheets", "v4", credentials=creds)
    return service


def get_sheet_raw(service, sheet_id, range):
    sheet = service.spreadsheets()
    try:
        sheet_data = sheet.values().get(spreadsheetId=sheet_id, range=range).execute()
    except HttpError as err:
        print(err)
    return sheet_data


def _package_batch_update(rows, columns, values, sheet_name=None):
    rows = np.atleast_1d(rows)
    columns = np.atleast_1d(columns)
    values = np.atleast_1d(values)
    data = []
    for r, c, v in zip(rows, columns, values):
        data.append(
            {
                "range": set_range((r, r), (c, c), sheet_name=sheet_name),
                "values": [[v]],
            }
        )
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    return body


def update_cells(rows, columns, values, service, sheet_id, sheet_name=None):
    sheet = service.spreadsheets()
    body = _package_batch_update(rows, columns, values, sheet_name)
    try:
        sheet.values().batchUpdate(spreadsheetId=sheet_id, body=body).execute()
    except HttpError as err:
        print(err)


def _package_append_data(data, add_blank_rows):
    values = []
    for row in data:
        if add_blank_rows:
            values.append(["" for x in row])
        values.append([str(x) for x in row])
    body = {"values": values}
    return body


def append_cells(data, range, service, sheet_id, add_blank_rows=False):
    sheet = service.spreadsheets()
    body = _package_append_data(data, add_blank_rows)

    try:
        sheet.values().append(
            spreadsheetId=sheet_id,
            range=range,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()
    except HttpError as err:
        print(err)


def parse_range(range_str):
    grp = re.search("(.*)!([A-Z]*)(\d*):", range_str)
    sheet = grp.groups()[0]
    base_col = grp.groups()[1]
    base_row = int(grp.groups()[2])
    return base_row, base_col, sheet


def set_range(rows, columns, sheet_name=None):
    colrow_range = (
        f"{columns[0]}{rows[0]}:{columns[1]}{rows[1] if rows[1] is not None else ''}"
    )
    if sheet_name is None:
        return colrow_range
    else:
        return f"{sheet_name}!{colrow_range}"


def process_records(data, columns, validation_map):
    records = []
    for row in data:
        record = {}
        for ii, k in enumerate(columns):
            if ii < len(row):
                record[k] = validation_map.get(k, lambda x: x)(row[ii])
            else:
                record[k] = pd.NA
        records.append(record)
    return pd.DataFrame.from_records(records, columns=columns)
