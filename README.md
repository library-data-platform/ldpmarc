ldpmarc
=======

Copyright (C) 2021 The Open Library Foundation  

This software is distributed under the terms of the Apache License, 
Version 2.0.  See the file [LICENSE](LICENSE) for more information.


__The ldpmarc tool converts LDP SRS/MARC records from JSON to a 
tabular format.__


System requirements
-------------------

* Operating systems supported:
  * Linux
* Database systems supported:
  * [PostgreSQL](https://www.postgresql.org/) 12.6 or later
    * required module: pg_trgm
* LDP compatibility:
  * [LDP](https://github.com/library-data-platform/ldp) 1.3 or later
* Required to build from source code:
  * [Go](https://golang.org/) 1.17 or later
* Required to build and run via Docker:
  * [Docker](https://docker.com) 17.05 or later


Enabling the pg_trgm module
---------------------------

The pg_trgm module should be enabled in the database by a superuser
(typically the `postgres` user):

```sql
CREATE EXTENSION pg_trgm;
```

If ldpmarc is not able to use the extension, it will generate this
error:

```
ERROR:  operator class "gin_trgm_ops" does not exist for access method "gin"
```

If this error appears even after the module has been enabled using
`CREATE EXTENSION`, it may be necessary to use the `GRANT` command to
grant permission for the extension to the LDP database administrator
(defined as `database_user`, in `ldpconf.json` under `ldp_database`).


Building ldpmarc
----------------

Set the `GOPATH` environment variable to specify a path that can serve 
as the build workspace for Go, e.g.:

```bash
$ export GOPATH=$HOME/go
```

Then:

```bash
$ ./build.sh
```

The `build.sh` script creates a `bin/` subdirectory and builds the
`ldpmarc` executable there.  To see all command-line options:

```bash
$ ./bin/ldpmarc -h
```

To build ldpmarc with Docker:

```bash
$ git clone https://github.com/library-data-platform/ldpmarc
$ cd ldpmarc
$ docker build -t ldpmarc:[VERSION] . 
```


Running ldpmarc
---------------

The most common usage is:

```bash
ldpmarc -D <datadir> -u <ldp_user>
```

where `datadir` is an LDP data directory containing `ldpconf.json`,
and `ldp_user` is the LDP user to be granted select privileges on the
output table.

For example:

```bash
$ ldpmarc -D ldp_data -u ldp
```

To run ldpmarc as a Docker container, omit the `-D` option and instead
mount your local LDP data directory at `/var/lib/ldp` in the
container:

```bash
$ docker run --rm -v /my/local/data/dir:/var/lib/ldp ldpmarc:<tag> -u <ldp_user>
```

SRS MARC data are read from the database tables `public.srs_marc` and
`public.srs_records`, and transformed into tabular data.  Only records
considered to be current are transformed, where current is defined as
having state = `ACTUAL` and an identifier present in `999$i`.

The transformed output is written to the table `public.srs_marctab`.

This process can take a long time to run and uses a lot of disk space
in the database.  In some libraries the output table may contain more
than 500 million rows and ldpmarc could use 200 GB of disk space or
more during the data loading process.


Full vs. incremental update
---------------------------

The first time ldpmarc runs, it will perform a "full update" of all of
the SRS records.  In subsequent runs, it will attempt to use
"incremental update" to update only records that have changed since
the previous run, which can dramatically reduce the running time if
the number of changes is small.

However, if very many records have changed, it is possible that
incremental update may take longer than full update.  In that case a
full update should be run once before resuming incremental updates.
This can be done by using the `-f` command-line option, which disables
incremental update and requires ldpmarc to do a full update.


Resetting ldpmarc
-----------------

All ldpmarc data can be deleted from the database by dropping three
tables:

```sql
DROP TABLE IF EXISTS dbsystem.ldpmarc_cksum, dbsystem.ldpmarc_metadata, public.srs_marctab;
```

This may be useful for uninstalling ldpmarc, or to restart it with a
blank slate.


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

