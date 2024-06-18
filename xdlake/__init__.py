import os
from uuid import uuid4

import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet

from xdlake import storage, delta_log, utils


def read_versioned_log_entries(url: storage.Location | str, version: int | None = None, storage_options: dict | None = None) -> dict[int, delta_log.DeltaLogEntry]:
    loc = storage.Location.with_loc(url)
    fs = storage.get_filesystem(loc.scheme, storage_options)

    log_loc = loc.append_path("_delta_log")
    if not fs.exists(log_loc.path):
        return {}
    versioned_log_entries = dict()
    for entry_loc in storage.list_files_sorted(log_loc, fs):
        entry_version = int(entry_loc.basename().split(".")[0])
        with storage.open(entry_loc, fs) as fh:
            versioned_log_entries[entry_version] = delta_log.DeltaLogEntry(fh)
        if version in versioned_log_entries:
            break
    return versioned_log_entries

class Writer:
    def __init__(self, loc: storage.Location | str, storage_options: dict | None = None):
        self.loc = storage.Location.with_loc(loc)
        self.fs = storage.get_filesystem(self.loc.scheme, storage_options)

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
                    size=self.fs.size(visited_file.path),
                    stats=stats.json(),
                    partitionValues=partition_values
                )
            )

        pyarrow.dataset.write_dataset(
            table,
            self.loc.path,
            format="parquet",
            filesystem=self.fs,
            basename_template=f"{version}-{uuid4()}-{{i}}.parquet",
            file_visitor=visitor,
            existing_data_behavior="overwrite_or_ignore",
            ** write_kwargs,
        )

        return add_actions

    def write(
        self,
        df: pa.Table,
        mode: str | delta_log.WriteMode = delta_log.WriteMode.append.name,
        partition_by: list | None = None,
        storage_options: dict | None = None,
    ):
        mode = delta_log.WriteMode[mode] if isinstance(mode, str) else mode
        schema_info = delta_log.Schema.from_pyarrow_table(df)
        versioned_log_entries = read_versioned_log_entries(self.loc, self.fs)
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
            self.fs.mkdir(os.path.join(self.loc.path, "_delta_log"))
        elif delta_log.WriteMode.append == mode:
            dlog = delta_log.DeltaLogEntry.AppendTable(partition_by, new_add_actions)
        elif delta_log.WriteMode.overwrite == mode:
            existing_add_actions = delta_log.resolve_add_actions(versioned_log_entries).values()
            dlog = delta_log.DeltaLogEntry.OverwriteTable(partition_by, existing_add_actions, new_add_actions)

        with storage.open(self.loc.append_path("_delta_log", f"{new_table_version:020}.json"), self.fs, "w") as fh:
            dlog.write(fh)

class DeltaTable:
    def __init__(self, url: str, storage_options: dict | None = None):
        self.loc = storage.Location.with_loc(url)
        self.fs = storage.get_filesystem(self.loc.scheme, storage_options)
        self.log = read_versioned_log_entries(self.loc, self.fs)
        self.adds = delta_log.resolve_add_actions(self.log)

    @property
    def version(self) -> int:
        if not self.log:
            return -1
        return max(self.log.keys())

    def to_pyarrow_dataset(self):
        paths = [self.loc.append_path(path).path for path in self.adds]
        return pyarrow.dataset.dataset(
            paths,
            format="parquet",
            partitioning="hive",
        )
