## Usage

```
./backiss backup --verbose --year=2019 --rules=./datahub_all.json
```

```
usage: backiss [-h] [--rules [RULES [RULES ...]]]
               [--filter-source-path FILTER_SOURCE_PATH] [--dry-run]
               [--year YEAR] [--month MONTH] [--day DAY] [-v]
               action

Backup as a kiss!

positional arguments:
  action                backup / restore / status / delete / expire

optional arguments:
  -h, --help            show this help message and exit
  --rules [RULES [RULES ...]]
                        rule definition (JSON format)
  --filter-source-path FILTER_SOURCE_PATH
                        filter source path, by regex
  --dry-run             dont do anything really
  --year YEAR           optional year
  --month MONTH         optional year
  --day DAY             optional year
  -v, --verbose         increase output verbosity
```

## Rule JSON schema

Backup from HDFS to local file-system:

```
{
  "source": {
    "file_system": "hdfs",
    "paths": [
      "/user/X/map/{path_date}",
      "/user/X/music/{path_date}"
    ]
  },
  "rewrite": {
    "^/user/X/(.*)$": "/backup/datahub/\\1"
  },
  "target": {
    "file_system": "file"
  }
}

```

## path_date

3 CLI args: "year" "month" "day" allow you to build a path like

    /year=2019/month=10
