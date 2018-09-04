Pipeline
===================
Re-usable pipeline to append CSV data from an HTTP URL into a MySQL database.

# Testing with watch
docker-compose -f docker-compose.test.yml run pipeline ptw

# Testing with coverage
docker-compose -f docker-compose.test.yml run pipeline pytest --cov=lib

# Running with docker-compose
1. Update Dockerfile CMD
2. docker-compose build
3. docker-compose run pipeline

# Running with python
./pipeline.py --source-url <url> --sink-user root --sink-password password --sink-host mysql --sink-database external --sink-table npi

# This version supports the following:

- Primary key definition with --pk_idx=0 --pk_type='int'.
- Dumps and archives for each table.
  A given dump from source 'x' to table 'xyz', would result in these tables being created: 'xyz_dumps' (dumps table), 'xyz' (main table) and 'xyz_archives' (archive table).

- Dump status and file line tracking to restart dumps when connection failed, program failed, etc. (from tables.field: xyz_dumps.status and xyz.line)

# TODO

- Understand better how to map mysql service from the docker container into the local system to persist all data.
- Test both python 2/3 (tox). Only python 2 tested for now.
- Lint the code and clean more (flake8)
- Improve performance. Explore batch inserts, multiple connections/cursors? cluster option?
- Support processing of a failed dump. Restarting inserts after the last successful line.

# Possible enhancements

- Show some sort of progress bar.
- Create database if database does not exist.
- Support multiple fields as a primary key; composite keys.
- Support field type definitions for all columns.
- Support definition of indexes somehow.
- Support option to halt on errors or log the error and continue.
- Error table ('xyz_errors') to keep track of lines that generate exceptions.
