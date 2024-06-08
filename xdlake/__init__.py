import os
from uuid import uuid4
from urllib.parse import urlparse

import fsspec
import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet

from xdlake import delta_log, utils


class StorageLocation:
    def __init__(self, url: str, **storage_options):
        parsed = urlparse(url)
        self.scheme = parsed.scheme or "file"
        self.fs = fsspec.filesystem(self.scheme, **storage_options)

        if "file" == self.scheme:
            if parsed.path.startswith(os.path.sep):
                self.path = parsed.path
            else:
                self.path = os.path.abspath(parsed.path)
        else:
            self.path = url

    def append_path(self, *path_components) -> str:
        if "file" == self.scheme:
            p = os.path.join(self.path, *path_components)
        else:
            p = "/".join([self.path, *path_components])
        return p

    @property
    def url(self, *path_components):
        p = self.append_path(*path_components)
        if "file" == self.scheme:
            return f"{self.scheme}://{p}"
        else:
            return p

    def open(self, p: str, mode: str="r") -> fsspec.core.OpenFile:
        if "file" == self.scheme and "w" in mode:
            folder = os.path.dirname(p)
            if self.fs.exists(folder):
                if not self.fs.isdir(folder):
                    raise FileExistsError(p)
            else:
                self.fs.mkdir(folder)
        return self.fs.open(p, mode)

def read_deltalog(loc: StorageLocation, storage_options: dict | None = None) -> dict[int, dict]:
    log_url = loc.append_path("_delta_log")
    if not loc.fs.exists(log_url):
        return []
    filepaths = sorted([file_info["name"] for file_info in loc.fs.ls(log_url, detail=True)
                        if "file" == file_info["type"]])
    log_entries = dict()
    for filepath in filepaths:
        version = int(os.path.basename(filepath).split(".")[0])
        with loc.open(filepath) as fh:
            log_entries[version] = delta_log.DeltaLog(fh)
    return log_entries

def _write_table(table: pa.Table, loc: StorageLocation, version: int, **write_kwargs):
    add_actions = list()

    def visitor(visited_file):
        stats = delta_log.Statistics.from_parquet_file_metadata(
            pyarrow.parquet.ParquetFile(visited_file.path).metadata
        )

        relpath = visited_file.path.replace(loc.path, "").strip("/")
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
        loc.path,
        format="parquet",
        filesystem=loc.fs,
        basename_template=f"{version}-{uuid4()}-{{i}}.parquet",
        file_visitor=visitor,
        existing_data_behavior="overwrite_or_ignore",
        ** write_kwargs,
    )

    return add_actions

def write(url: str, df: pa.Table, storage_options: dict | None = None, partition_by: list | None = None) -> dict:
    loc = StorageLocation(url, **(storage_options or dict()))
    schema_info = delta_log.Schema.from_pyarrow_table(df)

    log_info = read_deltalog(loc, storage_options)
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
        dlog.actions.append(delta_log.TableCommitCreate.with_parms(loc.path, utils.timestamp(), table_metadata, protocol))
        loc.fs.mkdir(os.path.join(loc.path, "_delta_log"))
    else:
        dlog.actions.extend(add_actions)
        dlog.actions.append(delta_log.TableCommitWrite.with_parms(utils.timestamp(), mode="Append", partition_by=partition_by))
    with loc.open(loc.append_path("_delta_log", f"{version:020}.json"), "w") as fh:
        dlog.write(fh)


class DeltaTable:
    def __init__(self, url: str, storage_options: dict | None = None):
        self.loc = StorageLocation(url)
        self._log_info = read_deltalog(self.loc, **(storage_options or dict()))

    def to_pyarow_dataset(self):
        adds = list()

        for version, dl in self._log_info.items():
            adds.extend(dl.add_actions())

        paths = [self.loc.append_path(a.path)
                 for a in adds]

        return pyarrow.dataset.dataset(
            paths,
            format="parquet",
            partitioning="hive",
        )
