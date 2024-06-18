import os
from uuid import uuid4
from urllib.parse import urlparse

import fsspec
import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet

from xdlake import delta_log, utils


class StorageLocation:
    def __init__(self, url: str, storage_options: dict | None = None):
        parsed = urlparse(url)
        self.scheme = parsed.scheme or "file"
        self.fs = fsspec.filesystem(self.scheme, **(storage_options or dict()))

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

def read_versioned_log_entries(loc: StorageLocation, version: int | None = None) -> dict[int, delta_log.DeltaLogEntry]:
    log_url = loc.append_path("_delta_log")
    if not loc.fs.exists(log_url):
        return {}
    filepaths = sorted([file_info["name"] for file_info in loc.fs.ls(log_url, detail=True)
                        if "file" == file_info["type"]])
    versioned_log_entries = dict()
    for filepath in filepaths:
        entry_version = int(os.path.basename(filepath).split(".")[0])
        with loc.open(filepath) as fh:
            versioned_log_entries[entry_version] = delta_log.DeltaLogEntry(fh)
        if version in versioned_log_entries:
            break
    return versioned_log_entries

class Writer:
    def __init__(self, loc: StorageLocation | str, storage_options: dict | None = None):
        if isinstance(loc, StorageLocation):
            self.loc = loc
        elif isinstance(loc, str):
            self.loc = StorageLocation(loc, storage_options)
        else:
            raise ValueError(f"Cannot handle storage location '{loc}'")

    def write_data(self, table: pa.Table, version: int, **write_kwargs) -> list[delta_log.Add]:
        add_actions = list()

        def visitor(visited_file):
            stats = delta_log.Statistics.from_parquet_file_metadata(
                pyarrow.parquet.ParquetFile(visited_file.path).metadata
            )

            relpath = visited_file.path.replace(self.loc.path, "").strip("/")
            partition_values = dict()

            for part in relpath.split("/"):
                if "=" in part:
                    key, value = part.split("=")
                    partition_values[key] = value

            add_actions.append(
                delta_log.Add(
                    path=relpath,
                    modificationTime=utils.timestamp(),
                    size=self.loc.fs.size(visited_file.path),
                    stats=stats.json(),
                    partitionValues=partition_values
                )
            )

        pyarrow.dataset.write_dataset(
            table,
            self.loc.path,
            format="parquet",
            filesystem=self.loc.fs,
            basename_template=f"{version}-{uuid4()}-{{i}}.parquet",
            file_visitor=visitor,
            existing_data_behavior="overwrite_or_ignore",
            ** write_kwargs,
        )

        return add_actions

    def write(
        self,
        df: pa.Table,
        mode: str = delta_log.WriteMode.append.name,
        partition_by: list | None = None,
        storage_options: dict | None = None,
    ):
        mode = delta_log.WriteMode[mode]
        schema_info = delta_log.Schema.from_pyarrow_table(df)
        versioned_log_entries = read_versioned_log_entries(self.loc)
        if not versioned_log_entries:
            new_table_version = 0
        else:
            new_table_version = 1 + max(versioned_log_entries.keys())
            if delta_log.WriteMode.error == mode:
                raise FileExistsError(f"Table already exists at version {new_table_version - 1}")
            elif delta_log.WriteMode.ignore == mode:
                return

        write_kwargs: dict = dict()
        if partition_by is not None:
            write_kwargs["partitioning"] = partition_by
            write_kwargs["partitioning_flavor"] = "hive"
        else:
            partition_by = list()

        new_add_actions = self.write_data(df, new_table_version, **write_kwargs)

        dlog = delta_log.DeltaLogEntry()
        if 0 == new_table_version:
            dlog = delta_log.DeltaLogEntry.CreateTable(self.loc.path, schema_info, partition_by, new_add_actions)
            self.loc.fs.mkdir(os.path.join(self.loc.path, "_delta_log"))
        elif delta_log.WriteMode.append == delta_log.WriteMode[mode]:
            dlog = delta_log.DeltaLogEntry.AppendTable(partition_by, new_add_actions)
        elif delta_log.WriteMode.overwrite == delta_log.WriteMode[mode]:
            existing_add_actions = delta_log.resolve_add_actions(versioned_log_entries).values()
            dlog = delta_log.DeltaLogEntry.OverwriteTable(partition_by, existing_add_actions, new_add_actions)

        with self.loc.open(self.loc.append_path("_delta_log", f"{new_table_version:020}.json"), "w") as fh:
            dlog.write(fh)

class DeltaTable:
    def __init__(self, url: str, storage_options: dict | None = None):
        self.loc = StorageLocation(url, storage_options)
        self.log = read_versioned_log_entries(self.loc)
        self.adds = delta_log.resolve_add_actions(self.log)

    @property
    def version(self) -> int:
        if not self.log:
            return -1
        return max(self.log.keys())

    def to_pyarrow_dataset(self):
        paths = [self.loc.append_path(path) for path in self.adds]
        return pyarrow.dataset.dataset(
            paths,
            format="parquet",
            partitioning="hive",
        )
