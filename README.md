ldpmarc
=======

Copyright (C) 2021-2022 The Open Library Foundation  

This software is distributed under the terms of the Apache License, 
Version 2.0.  See the file [LICENSE](LICENSE) for more information.


__The ldpmarc tool converts SRS/MARC records in LDP1, Metadb, or
LDLite from JSON to a tabular format.__


System requirements
-------------------

### Hardware

* Database storage (estimated): 500 GB
* Local storage (estimated): 500 GB

### Software

* Linux
* [PostgreSQL](https://www.postgresql.org/) 13.7 or later
  * PostgreSQL 14.3 or later is recommended
  * AWS RDS PostgreSQL is supported; Aurora is not supported
* [pg_trgm](https://www.postgresql.org/docs/current/pgtrgm.html) (PostgreSQL module)
* One of the following SRS data sources:
  * [LDP1](https://github.com/library-data-platform/ldp) 1.6.0 or later
  * [Metadb](https://github.com/metadb-project/metadb) 0.11
  * [LDLite](https://github.com/library-data-platform/ldlite)
* Required to build from source:
  * [Go](https://golang.org/) 1.18 or later
* Required to build and run via Docker:
  * [Docker](https://docker.com) 17.05 or later

### Other requirements

The ldpmarc software currently supports MARC 21 data.


Database configuration
----------------------

### PostgreSQL settings

The PostgreSQL setting `max_locks_per_transaction` should be increased
to avoid an "out of shared memory" error.  The recommended setting is:

* `max_locks_per_transaction`: `1100`

### Enabling pg_trgm

The pg_trgm module is used to support the SQL `LIKE` and `ILIKE`
pattern matching operators on MARC content data.  The module is
enabled in the database by a superuser:

```
CREATE EXTENSION pg_trgm WITH SCHEMA pg_catalog;
```

Then grant the LDP1 administrator permission to use the extension,
e.g.:
```
GRANT USAGE ON SCHEMA public TO ldpadmin;
```

To test if the extension is working, log in as the ldpadmin user and
run:

```
CREATE TEMP TABLE t (v varchar(1));

CREATE INDEX ON t USING GIN (v gin_trgm_ops);
```

If this shows any errors, then the extension may not be enabled.

If the pg_trgm extension is not enabled, the `-T` option must be
included for ldpmarc to disable trigram indexes, but this will
significantly impact query performance when using the `LIKE` or
`ILIKE` operator with the `content` column.


Building ldpmarc
----------------

Set the `GOPATH` environment variable to specify a path that can serve 
as the build workspace for Go, e.g.:

```
export GOPATH=$HOME/go
```

Then:

```
./build.sh
```

The `build.sh` script creates a `bin/` subdirectory and builds the
`ldpmarc` executable there.  To see all command-line options:

```
./bin/ldpmarc -h
```


Running ldpmarc with LDP1
-------------------------

The most common usage is:

```
ldpmarc -D <datadir> -u <ldp1_user>
```

where `datadir` is a LDP1 data directory containing `ldpconf.json`,
and `ldp1_user` is a LDP1 user to be granted select privileges on the
output table.  Note that at present ldpmarc only grants privileges for
a single user.

For example:

```
ldpmarc -D ldp1_data -u ldp
```

SRS MARC data are read from the database tables `public.srs_marc` and
`public.srs_records`, and transformed into tabular data.  Only records
considered to be current are transformed, where current is defined as
having state = `ACTUAL` and an identifier present in `999$i`.

The transformed output is written to the table `public.srs_marctab`.

This process can take a long time to run and uses a lot of disk space
in the database.

If individual user accounts are configured for LDP1, a shell script
can be used to grant privileges to the users, for example:

```
users=/path/to/list/of/users.txt
for u in $( cat $users ); do
    psql -c "GRANT SELECT ON public.srs_marctab TO $u ;"  (etc.)
done
```


Running ldpmarc with Metadb 0.12
--------------------------------

The usage for ldpmarc with Metadb 0.12 is similar to running it with
LDP1.

When running ldpmarc, the `-M` option should be added to enable
compatibility with Metadb, for example:


```
ldpmarc -D data -M
```

The transformed output is written to the table `folio_source_record.marctab`.

To grant privileges to Metadb users:

```
users=/path/to/list/of/users.txt
for u in $( cat $users ); do
    psql -c "GRANT SELECT ON folio_source_record.marctab TO $u ;"  (etc.)
done
```


Full vs. incremental update
---------------------------

The first time ldpmarc runs, it will perform a "full update" of all of
the SRS records.  In subsequent runs, it will attempt to use
"incremental update" to update only records that have changed since
the previous run, which can dramatically reduce the running time if
the number of changes is small.

However, if very many records have changed, it is possible that
incremental update may take longer than full update.  If it appears
that an incremental update will never finish, it should be canceled,
and a full update should be run once before resuming incremental
updates.  This can be done by using the `-f` command-line option,
which disables incremental update and requires ldpmarc to do a full
update.

If a very large number of records are changed routinely, it may be
faster not to use incremental update at all and instead to run only
full update every time.  In this case the `-F` option should be used
(instead of `-f`) which will optimize storage for full updates only.


Resetting ldpmarc
-----------------

All ldpmarc data can be deleted from the database by dropping these
tables:

```
DROP TABLE IF EXISTS folio_source_record.marctab, marctab._srs_marctab, marctab.cksum, marctab.metadata, public.srs_marctab;
```

This may be useful for uninstalling ldpmarc, or to restart it with a
blank slate.


Running ldpmarc with Docker
---------------------------

To build ldpmarc with Docker:

```
git clone https://github.com/library-data-platform/ldpmarc

cd ldpmarc

docker build -t ldpmarc:[VERSION] . 
```

To run ldpmarc as a Docker container, omit the `-D` option and instead
mount your local LDP1 data directory at `/var/lib/ldp` in the
container:

```
docker run --rm -v /my/local/data/dir:/var/lib/ldp ldpmarc:<tag> -u <ldp1_user>
```


Resources
---------

* Report bugs at
  [Issues](https://github.com/library-data-platform/ldpmarc/issues)

* Ask questions at
  [Discussions](https://github.com/library-data-platform/ldpmarc/discussions)

* For notification of new releases, on the [ldpmarc page in
  GitHub](https://github.com/library-data-platform/ldpmarc) select
  Watch > Custom > Releases.

* Notes on [using ldpmarc with
  LDLite](https://github.com/library-data-platform/ldlite/blob/main/srs.md)

