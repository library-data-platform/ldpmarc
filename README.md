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
* [Go](https://golang.org/) 1.16 or later
* [LDP](https://github.com/library-data-platform/ldp) 1.2 or later


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
ldpmarc -f <odbc_file> -d <dsn> -u <ldp_user>
```

where `odbc_file` is an ODBC data source file name such as
`$HOME/.odbc.ini` or `/etc/odbc.ini`, `dsn` is the ODBC data source
name for the LDP database, and `ldp_user` is the LDP user to be
granted select privileges on the output table.

For example:

```bash
$ ldpmarc -f ~/.odbc.ini -d ldp_db -u ldp
```

SRS MARC data are read from the database table `public.srs_marc` and
transformed into tabular data, in a format similar to one used at the
University of Chicago.  The output is written to the table
`folio_source_record.__marc`.

The process can take a long time to run and uses a lot of disk space
in the database.  In some libraries the output table may contain more
than 500 million rows and ldpmarc could use 200 GB of disk space or
more during the data loading process.


