#!/bin/sh
set -e

clean_f='false'
lint_f='true'
verbose_f='false'

usage() {
	echo ''
	echo 'Usage:  build.sh [<flags>]'
	echo ''
	echo 'Flags:'
	echo '-c                            - Clean (remove executable) before building'
	echo '-h                            - Help'
	echo '-L                            - Do not run linters (golangci-lint)'
	echo '-v                            - Enable verbose output'
}

while getopts 'Lchtv' flag; do
    case "${flag}" in
        L) lint_f='false' ;;
        c) clean_f='true' ;;
        h) usage
            exit 1 ;;
        v) verbose_f='true' ;;
        *) usage
            exit 1 ;;
    esac
done

shift $(($OPTIND - 1))
for arg; do
    if [ $arg = 'help' ]
    then
        usage
        exit 1
    fi
    echo "build.sh: unknown argument: $arg" 1>&2
    exit 1
done

if $verbose_f; then
    v='-v'
fi

if $lint_f; then
    echo 'build.sh: running linters' 1>&2
    golangci-lint run $v 1>&2
fi

bindir=bin

if $clean_f; then
    echo 'build.sh: removing executable' 1>&2
    rm -f ./$bindir/ldpmarc
fi

echo 'build.sh: compiling ldpmarc' 1>&2

mkdir -p $bindir

command=ldpmarc
go build -o $bindir $v -race ./cmd/$command

echo 'build.sh: compiled to executable in bin' 1>&2

