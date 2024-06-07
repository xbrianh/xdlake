import os
import json
import fsspec
from datetime import datetime, timezone
from uuid import uuid4
from urllib.parse import urlparse
from collections import defaultdict

import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet


def timestamp(dt: datetime | None = None) -> int:
    dt = dt or datetime.now(timezone.utc)
    return int(dt.timestamp() * 1000)

CLIENT_VERSION = "sdlake-0.0.0"

PROTOCOL_ACTION = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}

METADATA = {
  "metaData": {
    "id": "988aef4e-183e-463e-8b83-3bec352729d2",
    "name": None,
    "description": None,
    "format": {
      "provider": "parquet",
      "options": {}
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"0\",\"type\":\"double\",\"nullable\":True,\"metadata\":{}},{\"name\":\"1\",\"type\":\"double\",\"nullable\":True,\"metadata\":{}},{\"name\":\"2\",\"type\":\"double\",\"nullable\":True,\"metadata\":{}},{\"name\":\"3\",\"type\":\"double\",\"nullable\":True,\"metadata\":{}},{\"name\":\"4\",\"type\":\"double\",\"nullable\":True,\"metadata\":{}}]}",
    "partitionColumns": [],
    "createdTime": 1717277177384,
    "configuration": {}
  }
}

ADD = {
  "add": {
    "path": "0-62b0f8cb-6991-4f13-86c1-0530822e9378-0.parquet",
    "partitionValues": {},
    "size": 2414,
    "modificationTime": 1717277177384,
    "dataChange": True,
    "stats": "{\"numRecords\": 11, \"minValues\": {\"0\": 0.037964144340130956, \"1\": 0.060449978031596574, \"2\": 0.03946171954196798, \"3\": 0.004219535424763832, \"4\": 0.01191401722014973}, \"maxValues\": {\"0\": 0.9719751853295551, \"1\": 0.9989695414962797, \"2\": 0.8319658722321173, \"3\": 0.8745623818957149, \"4\": 0.9832467029023835}, \"nullCount\": {\"0\": 0, \"1\": 0, \"2\": 0, \"3\": 0, \"4\": 0}}",
    "tags": None,
    "deletionVector": None,
    "baseRowId": None,
    "defaultRowCommitVersion": None,
    "clusteringProvider": None
  }
}


COMMIT_INFO = {"commitInfo": {
    "timestamp": 1717277177384,
    "operation": "CREATE TABLE",
    "operationParameters": {
      "location": "file:///workspace/xdlake/tdl",
      "protocol": "{\"minReaderVersion\":1,\"minWriterVersion\":2}",
      "mode": "ErrorIfExists",
      "metadata": "{\"configuration\":{},\"createdTime\":1717277177384,\"description\":null,\"format\":{\"options\":{},\"provider\":\"parquet\"},\"id\":\"988aef4e-183e-463e-8b83-3bec352729d2\",\"name\":None,\"partitionColumns\":[],\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"0\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"1\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"2\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"3\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"4\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\"}"
    },
    "clientVersion": "delta-rs.0.17.3"
  }
}


def _create_table_commit(location: str, timestamp: int, metadata: dict, protocol: dict):
    info = {
        "timestamp": timestamp,
        "operation": "CREATE TABLE",
        "operationParameters": {
            "metadata": json.dumps(metadata),
            "protocol": json.dumps(protocol),
            "location": location,
            "mode": "ErrorIfExists",
        },
        "clientVersion": CLIENT_VERSION,
    }
    return {"commitInfo": info}


def _get_filesystem(url: str, storage_options: dict | None = None) -> tuple:
    parsed = urlparse(url)
    scheme = parsed.scheme or "file"
    fs = fsspec.filesystem(scheme, **(storage_options or dict()))
    if "file" == scheme:
        filepath = os.path.abspath(parsed.path)
        url = f"file://{filepath}"
    else:
        filepath = None
    return url, filepath, fs


def _schema_info(df: pa.Table) -> dict:
    info = {
        "type": "struct",
        "fields": [
            {
                "name": f.name,
                 "type": str(f.type),
                 "nullable": f.nullable,
                 "metadata": f.metadata if f.metadata else {}
            }
            for f in df.schema
        ]
    }
    return info


def _create_metadata(schema_info: dict):
    md = {
        "id": f"{uuid4()}",
        "name": None,
        "description": None,
        "format": {
          "provider": "parquet",
          "options": {}
        },
        "schemaString": json.dumps(schema_info),
        "partitionColumns": [],
        "createdTime": timestamp(),
        "configuration": {},
    }
    return md


ADD = {
  "add": {
    "path": "0-62b0f8cb-6991-4f13-86c1-0530822e9378-0.parquet",
    "partitionValues": {},
    "size": 2414,
    "modificationTime": 1717277177384,
    "dataChange": True,
    "stats": "{\"numRecords\": 11, \"minValues\": {\"0\": 0.037964144340130956, \"1\": 0.060449978031596574, \"2\": 0.03946171954196798, \"3\": 0.004219535424763832, \"4\": 0.01191401722014973}, \"maxValues\": {\"0\": 0.9719751853295551, \"1\": 0.9989695414962797, \"2\": 0.8319658722321173, \"3\": 0.8745623818957149, \"4\": 0.9832467029023835}, \"nullCount\": {\"0\": 0, \"1\": 0, \"2\": 0, \"3\": 0, \"4\": 0}}",
    "tags": None,
    "deletionVector": None,
    "baseRowId": None,
    "defaultRowCommitVersion": None,
    "clusteringProvider": None
  }
}

def _create_add_action(path: str, timestamp_now: int, size: int, stats: dict):
    info = {
        "path": path,
        "partitionValues": {},
        "size": size,
        "modificationTime": timestamp_now,
        "dataChange": True,
        "stats": json.dumps(stats),
        "tags": None,
        "deletionVector": None,
        "baseRowId": None,
        "defaultRowCommitVersion": None,
        "clusteringProvider": None
    }
    return {"add": info}


def compile_statistics(md: dict):
    stats = dict()
    stats["numRecords"] = md["num_rows"]
    min_values = defaultdict(dict)
    max_values = defaultdict(dict)
    nullcounts = defaultdict(int)
    for rg_info in md["row_groups"]:
        for col_info in rg_info["columns"]:
            column = col_info["path_in_schema"]
            if col_info["statistics"]["has_min_max"]:
                if not min_values.get(column):
                    min_values[column] = col_info["statistics"]["min"]
                else:
                    min_values[column] = min(min_values[column], col_info["statistics"]["min"])
                if not max_values.get(column):
                    max_values[column] = col_info["statistics"]["max"]
                else:
                    max_values[column] = max(max_values[column], col_info["statistics"]["max"])
            nullcounts[column] += col_info["statistics"]["null_count"]
    stats["minValues"] = dict(min_values)
    stats["maxValues"] = dict(max_values)
    stats["nullCount"] = nullcounts
    return stats


def read_deltalog(url: str, storage_options: dict | None = None) -> dict[int, dict]:
    url, filepath, fs = _get_filesystem(url, storage_options)
    log_url = os.path.join(url, "_delta_log")
    if not fs.exists(log_url):
        return []
    filepaths = sorted([file_info["name"] for file_info in fs.ls(log_url, detail=True)
                        if "file" == file_info["type"]])
    log_entries = dict()
    for filepath in filepaths:
        name = os.path.basename(filepath)
        version = int(name.split(".")[0])
        with fs.open(filepath) as fh:
            for line in fh:
                log_entries[version] = json.loads(line)
    return log_entries


def write(url: str, df: pa.Table, storage_options: dict | None = None, partition_by: list | None = None) -> dict:
    url, filepath, fs = _get_filesystem(url, storage_options)
    schema_info = _schema_info(df)
    delta_log = read_deltalog(url, storage_options)
    if delta_log:
        version = 1 + max(delta_log.keys())
    else:
        version = 0

    write_kwargs = dict()
    if partition_by is not None:
        write_kwargs["partitioning"] = partition_by
        write_kwargs["partitioning_flavor"] = "hive"

    add_actions = dict()
    timestamp_now = timestamp()

    def visitor(visited_file):
        md = pyarrow.parquet.ParquetFile(visited_file.path).metadata
        stats = compile_statistics(md.to_dict())
        path = os.path.relpath(visited_file.path, filepath)
        add_actions[visited_file.path] = _create_add_action(path, timestamp_now, fs.size(visited_file.path), stats)

    pyarrow.dataset.write_dataset(
        df,
        filepath or url,
        format="parquet",
        filesystem=fs,
        basename_template=f"{version}-{uuid4()}-{{i}}.parquet",
        file_visitor=visitor,
        ** write_kwargs,
    )

    log_actions = list()
    if not delta_log:
        table_metadata = _create_metadata(schema_info)
        log_actions.append(PROTOCOL_ACTION)
        log_actions.append({"metaData": table_metadata})
        for action in add_actions.values():
            log_actions.append(action)
        if "file" == fs.protocol[0]:
            location = f"file://{filepath}"
        else:
            location = url
        log_actions.append(_create_table_commit(location, timestamp_now, table_metadata, PROTOCOL_ACTION))
        fs.mkdir(os.path.join(filepath, "_delta_log"))
        filepath = os.path.join(filepath, "_delta_log", f"{version:020}.json")
        with fs.open(filepath, "w") as fh:
            entries = [json.dumps(a) for a in log_actions]
            fh.write(os.linesep.join(entries))
