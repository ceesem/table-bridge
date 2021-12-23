# Table-Bridge

This is a set of tooling for bridging between google sheets and dataframes as well as comparing dataframes.
The use case for these seemingly dissimilar actions is to keep a google sheet up to date with the contents of a database in two ways.

1) Adding new rows to the google sheet when new database items are posted

2) Comparing the contents of the google sheet to the contents of the database table and taking appropriate actions.

The authentication process is heavily drawn from the [Sheets API Quickstart](https://developers.google.com/sheets/api/quickstart/python).
