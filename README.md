# Data-utilities
Functions for dealing with data

## Connectors
- sqldb: Postgres connector
- api: Mixpanel, Salesforce, and Zendesk connectors

## Extractors
- From DB, Mixpanel, or Zendesk.
- Simple salesforce allows for SOQL, which is similar to SQL.

## Loaders
- Copy data to Postgres.

## TODO
- Refactor extract methods to use a single API class.
- Write specialised methods for this class to deal with the different APIs.
- Zendesk result pagination.
- Use petl for loading data into a database.
