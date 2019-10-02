## Usage

python3 backiss.py backup --verbose --year=2019 --rules=./datahub_all.json

## Rule JSON schema

Backup from HDFS to local file-system:

```
{
  "source": {
    "file_system": "hdfs",
    "paths": [
      "/user/X/map/{path_date_suffix}",
      "/user/X/music/{path_date_suffix}"
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
