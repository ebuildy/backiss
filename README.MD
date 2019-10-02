## Usage

```
python3 backiss.py backup --verbose --year=2019 --rules=./datahub_all.json
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
