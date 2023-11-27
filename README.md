# StocksAnalysis
Final project of CSI-30 Course.

1. Download PostgreSQL:

2. Create Table with `create_tables.sql` file

3. Download Data:
```bash
python3 collector.py
```

4. Populate Tables:

You need the postgresql database with a admin password and running on port 5432. (postgresql://postgres:admin@localhost:5432/postgres)

```bash
python3 dw_populate.py
```