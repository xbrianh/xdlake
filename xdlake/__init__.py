import os
import fsspec
from uuid import uuid4
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet

from xdlake import delta_log, utils


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


def read_deltalog(url: str, storage_options: dict | None = None) -> dict[int, dict]:
    url, filepath, fs = _get_filesystem(url, storage_options)
    log_url = os.path.join(url, "_delta_log")
    if not fs.exists(log_url):
        return []
    filepaths = sorted([file_info["name"] for file_info in fs.ls(log_url, detail=True)
                        if "file" == file_info["type"]])
    log_entries = dict()
    for filepath in filepaths:
        version = int(os.path.basename(filepath).split(".")[0])
        with fs.open(filepath) as fh:
            log_entries[version] = delta_log.DeltaLog(fh)
    return log_entries


def write(url: str, df: pa.Table, storage_options: dict | None = None, partition_by: list | None = None) -> dict:
    url, filepath, fs = _get_filesystem(url, storage_options)
    schema_info = delta_log.Schema.from_pyarrow_table(df)
    log_info = read_deltalog(url, storage_options)
    if log_info:
        version = 1 + max(log_info.keys())
    else:
        version = 0

    write_kwargs = dict()
    if partition_by is not None:
        write_kwargs["partitioning"] = partition_by
        write_kwargs["partitioning_flavor"] = "hive"

    add_actions = list()
    timestamp_now = utils.timestamp()

    def visitor(visited_file):
        stats = delta_log.Statistics.from_parquet_file_metadata(
            pyarrow.parquet.ParquetFile(visited_file.path).metadata
        )
        path = os.path.relpath(visited_file.path, filepath)
        partition_values = dict()
        for part in path.split("/"):
            if "=" in part:
                key, value = part.split("=")
                partition_values[key] = value
        add_actions.append(
            delta_log.Add(
                path=path,
                modificationTime=timestamp_now,
                size=fs.size(visited_file.path),
                stats=stats.json(),
                partitionValues=partition_values)
        )

    pyarrow.dataset.write_dataset(
        df,
        filepath or url,
        format="parquet",
        filesystem=fs,
        basename_template=f"{version}-{uuid4()}-{{i}}.parquet",
        file_visitor=visitor,
        existing_data_behavior="overwrite_or_ignore",
        ** write_kwargs,
    )

    dlog = delta_log.DeltaLog()
    if not log_info:
        protocol = delta_log.Protocol()
        table_metadata = delta_log.TableMetadata(schemaString=schema_info.json(), partitionColumns=partition_by)
        dlog.append(protocol)
        dlog.append(table_metadata)
        for action in add_actions:
            dlog.append(action)
        if "file" == fs.protocol[0]:
            location = f"file://{filepath}"
        else:
            location = url
        dlog.append(delta_log.TableCommitCreate.with_parms(location, timestamp_now, table_metadata, protocol))
        fs.mkdir(os.path.join(filepath, "_delta_log"))
    else:
        for action in add_actions:
            dlog.append(action)
        dlog.append(delta_log.TableCommitWrite.with_parms(timestamp_now, mode="Append", partition_by=partition_by))
    filepath = os.path.join(filepath, "_delta_log", f"{version:020}.json")
    with fs.open(filepath, "w") as fh:
        dlog.write(fh)
