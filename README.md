## This project has some utility code for db2 databases.

### Some of the programs worth noting
- *IdentifyTableDifferencesInTwoDatabases* - Probably most useful; use this to compare the tables
between two different databases.  It compares tables/views and identifies if schema's differ; it also
identifies the record counts between the tables and identifies the 'percentage different'.  The log file
the code generates can be opened as a csv to allow easy review/sorting.
- *CompareTablesInDiffSchemas* - Use this if have two different schema's with same name tables and
want to see the difference in them.  I used it to compare an 'archive' schema to the 'production' one
- *GetTableInfo* - You pass in the table and it'll write out the column attributes for you, useful little util