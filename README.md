ldpmarc
=======

Copyright (C) 2021 The Open Library Foundation  

This software is distributed under the terms of the Apache License, 
Version 2.0.  See the file [LICENSE](LICENSE) for more information.


__The ldpmarc tool converts LDP SRS/MARC records from JSON to a 
tabular format.__


System requirements
-------------------

* Linux
* [PostgreSQL](https://www.postgresql.org/) 12.6 or later
  * required module: pg_trgm
* [Go](https://golang.org/) 1.16 or later
* [LDP](https://github.com/library-data-platform/ldp) 1.3 or later
* Optional: [Docker](https://docker.com) 17.05 or later

The pg_trgm module is enabled in the database by a superuser:

```sql
CREATE EXTENSION pg_trgm;
```

If the pg_trgm extension is not enabled, the `-T` option must be used
with ldpmarc to disable trigram indexes, and this will impact query
performance.


Building the software
---------------------

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


Building the software with Docker
---------------------------------

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

The default process is a "full update" which means that the entire
`srs_marctab` table and all of its indexes are rebuilt.  In ldpmarc
1.2, experimental support for "incremental update" is available.
Incremental update may run much faster than full update if the number
of changes is small.  (If many records have changed, incremental
update may take too much time; and in that case a full update should
be run once before resuming incremental updates.)  To enable
incremental update, use the `-i` command-line option.  Note that even
with `-i` ldpmarc will still perform a full update when required, such
as the first time that ldpmarc 1.2 is run, or when schema changes need
to be made.


