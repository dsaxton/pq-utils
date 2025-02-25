```
A utility tool for reading parquet files

Usage: pq-utils [COMMAND]

Commands:
  cat     Display the contents of a file
  head    Display the first n rows of a file [default: 10]
  schema  Display the schema of a file
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

The output format can be specified using `-f` or `--format`. Supported formats are:
  - csv (default)
  - json
