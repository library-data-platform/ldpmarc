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
`ldpmarc` executable there:

```bash
$ ./bin/ldpmarc -h
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

SRS MARC data are read from the database table `public.srs_marc` and
transformed into tabular data, in a format similar to one used at the
University of Chicago.  The output is written to the table
`folio_source_record.__marc`.

The process can take a long time to run and uses a lot of disk space
in the database.  In some libraries the output table may contain more
than 500 million rows and ldpmarc could use 200 GB of disk space or
more during the data loading process.


