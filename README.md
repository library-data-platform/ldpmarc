ldpmarc
=======

Copyright (C) 2021-2023 The Open Library Foundation  

This software is distributed under the terms of the Apache License, 
Version 2.0.  See the file [LICENSE](LICENSE) for more information.


__The ldpmarc tool converts SRS/MARC records in LDP1, Metadb, or
LDLite from JSON to a tabular format.__


System requirements
-------------------

### Hardware

* Database storage: 500 GB
* Local storage: 500 GB

### Software

* Linux
* [PostgreSQL](https://www.postgresql.org/) 13 or later
  * PostgreSQL 15 or later is recommended
  * AWS RDS PostgreSQL optionally may be used (with servers in the
    same zone/subnet); Aurora is not supported
* One of the following SRS data sources:
  * [LDP1](https://github.com/library-data-platform/ldp) 1.7 or later
  * [Metadb](https://github.com/metadb-project/metadb) (ldpmarc
    installation not required for Metadb 1.0 or later)
  * [LDLite](https://github.com/library-data-platform/ldlite)
* Required to build from source:
  * [Go](https://golang.org/) 1.19 or later
* Required to build and run via Docker:
  * [Docker](https://docker.com) 17.05 or later

### Other requirements

The ldpmarc software supports MARC 21 data.


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
ldpmarc -D data -u ldp
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


Running ldpmarc with Metadb
---------------------------

**Beginning with Metadb 1.0, ldpmarc is automatically installed and
run by Metadb.  The following instructions are for Metadb versions
earlier than 1.0.**

When running ldpmarc, the `-M` option should be added to enable
compatibility with Metadb, for example:


```
ldpmarc -D data -M
```

The transformed output is written to the table
`folio_source_record.marc__t`.  (Releases of ldpmarc 1.6 and earlier
write to the table `folio_source_record.marctab` instead.)

To grant privileges to Metadb users:

```
users=/path/to/list/of/users.txt
for u in $( cat $users ); do
    psql -c "GRANT SELECT ON folio_source_record.marc__t TO $u ;"  (etc.)
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


Resetting ldpmarc
-----------------

All ldpmarc data can be deleted from the database by dropping the
following tables.  This may be useful for completely resetting
ldpmarc.

For LDP1:
```
DROP TABLE IF EXISTS public.srs_marctab, marctab.cksum, marctab.metadata, marctab._srs_marctab;
```

For Metadb:
```
DROP TABLE IF EXISTS folio_source_record.marc__t, marctab.cksum, marctab.metadata, marctab._srs_marctab, folio_source_record.marctab;
```


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

