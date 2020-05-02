import os
if 'PORT' in os.environ:
  # elephantsql
    db = {
        "host": "satao.db.elephantsql.com",
        "db": "ajkkxcga",
        "user": "ajkkxcga",
        "passwd": "J4cZeMrHCUAJd0tEIUarjALSVQ8KBQtC",
        "port": "5432"
    }
else:
    db = {
        "host": "localhost",
        "db": "postgres",
        "user": "postgres",
        "passwd": "b7ec6b08dc1f4f3383663c0ecdf5dda7",
        "port": "5432"
    }
