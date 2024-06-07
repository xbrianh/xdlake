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

class _Location:
    def __init__(self, url: str, storage_options: dict | None = None):
        parsed = urlparse(url)
        self.scheme = parsed.scheme or "file"
        self.fs = fsspec.filesystem(self.scheme, **(storage_options or dict()))
        if "file" == self.scheme:
            self.filepath = os.path.abspath(parsed.path)
            self.url = f"file://{self.filepath}"
        else:
            self.filepath = None
            self.url = url

    def location(self) -> str:
        if "file" == self.fs.protocol[0]:
            return f"file://{self.filepath}"
        else:
            return self.url

def _write_table(table: pa.Table, loc: _Location, version: int, **write_kwargs):
    add_actions = list()

    def visitor(visited_file):
        stats = delta_log.Statistics.from_parquet_file_metadata(
            pyarrow.parquet.ParquetFile(visited_file.path).metadata
        )

        relpath = visited_file.path.replace(loc.filepath, "").strip("/")
        partition_values = dict()

        for part in relpath.split("/"):
            if "=" in part:
                key, value = part.split("=")
                partition_values[key] = value

        add_actions.append(
            delta_log.Add(
                path=relpath,
                modificationTime=utils.timestamp(),
                size=loc.fs.size(visited_file.path),
                stats=stats.json(),
                partitionValues=partition_values
            )
        )

    pyarrow.dataset.write_dataset(
        table,
        loc.filepath or loc.url,
        format="parquet",
        filesystem=loc.fs,
        basename_template=f"{version}-{uuid4()}-{{i}}.parquet",
        file_visitor=visitor,
        existing_data_behavior="overwrite_or_ignore",
        ** write_kwargs,
    )

    return add_actions

def write(url: str, df: pa.Table, storage_options: dict | None = None, partition_by: list | None = None) -> dict:
    loc = _Location(url, storage_options)
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

    add_actions = _write_table(df, loc, version, **write_kwargs)

    dlog = delta_log.DeltaLog()
    if not log_info:
        protocol = delta_log.Protocol()
        table_metadata = delta_log.TableMetadata(schemaString=schema_info.json(), partitionColumns=partition_by)
        dlog.actions.append(protocol)
        dlog.actions.append(table_metadata)
        dlog.actions.extend(add_actions)
        dlog.actions.append(delta_log.TableCommitCreate.with_parms(loc.location(), utils.timestamp(), table_metadata, protocol))
        loc.fs.mkdir(os.path.join(loc.filepath, "_delta_log"))
    else:
        dlog.actions.extend(add_actions)
        dlog.actions.append(delta_log.TableCommitWrite.with_parms(utils.timestamp(), mode="Append", partition_by=partition_by))
    with loc.fs.open("/".join([loc.filepath, "_delta_log", f"{version:020}.json"]), "w") as fh:
        dlog.write(fh)
