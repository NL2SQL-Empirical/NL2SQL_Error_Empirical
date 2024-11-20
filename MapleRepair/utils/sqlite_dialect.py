from sqlglot.dialects.sqlite import SQLite

SQLite_Dialects = SQLite()
SQLite_Dialects.IDENTIFIER_START = '`'
SQLite_Dialects.IDENTIFIER_END = '`'