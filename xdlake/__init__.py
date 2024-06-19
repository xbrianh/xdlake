from uuid import uuid4

import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet

from xdlake import storage, delta_log, utils


def read_versioned_log_entries(
    loc: str | storage.Location | storage.LocatedFS,
    version: int | None = None,
    storage_options: dict | None = None,
) -> dict[int, delta_log.DeltaLogEntry]:
    lfs = storage.LocatedFS.resolve(loc, storage_options)
    if not lfs.exists():
        return {}
    versioned_log_entries = dict()
    for entry_lfs in storage.list_files_sorted(lfs):
        entry_version = int(entry_lfs.loc.basename().split(".")[0])
        with storage.open(entry_lfs) as fh:
            versioned_log_entries[entry_version] = delta_log.DeltaLogEntry(fh)
        if version in versioned_log_entries:
            break
    return versioned_log_entries

class Writer:
    def __init__(
        self,
        loc: str | storage.Location | storage.LocatedFS,
        log_loc: str | storage.Location | storage.LocatedFS | None = None,
        storage_options: dict | None = None,
    ):
        self.lfs = storage.LocatedFS.resolve(loc, storage_options)
        if log_loc is None:
            self.log_lfs = self.lfs.append_path("_delta_log")
        else:
            self.log_lfs = storage.LocatedFS.resolve(log_loc, storage_options)

    def write_data(self, table: pa.Table, version: int, **write_kwargs) -> list[delta_log.Add]:
        add_actions = list()

        def visitor(visited_file):
            stats = delta_log.Statistics.from_parquet_file_metadata(
                pyarrow.parquet.ParquetFile(visited_file.path).metadata
            )

            relpath = visited_file.path.replace(self.lfs.path, "").strip("/")
            partition_values = dict()

            for part in relpath.split("/"):
                if "=" in part:
                    key, value = part.split("=")
                    partition_values[key] = value

            add_actions.append(
                delta_log.Add(
                    path=relpath,
                    modificationTime=utils.timestamp(),
                    size=self.lfs.fs.size(visited_file.path),
                    stats=stats.json(),
                    partitionValues=partition_values
                )
            )

        pyarrow.dataset.write_dataset(
            table,
            self.lfs.path,
            format="parquet",
            filesystem=self.lfs.fs,
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
        schema = delta_log.Schema.from_pyarrow_table(df)
        versioned_log_entries = read_versioned_log_entries(self.log_lfs)
        if not versioned_log_entries:
            new_table_version = 0
        else:
            new_table_version = 1 + max(versioned_log_entries.keys())
            if delta_log.WriteMode.error == mode:
                raise FileExistsError(f"Table already exists at version {new_table_version - 1}")
            elif delta_log.WriteMode.ignore == mode:
                return
            existing_schema = delta_log.resolve_schema(versioned_log_entries)
            if existing_schema != schema:
                raise ValueError("Schema mismatch")

        write_kwargs: dict = dict()
        if partition_by is not None:
            write_kwargs["partitioning"] = partition_by
            write_kwargs["partitioning_flavor"] = "hive"
        else:
            partition_by = list()

        new_add_actions = self.write_data(df, new_table_version, **write_kwargs)

        dlog = delta_log.DeltaLogEntry()
        if 0 == new_table_version:
            dlog = delta_log.DeltaLogEntry.CreateTable(self.log_lfs.path, schema, partition_by, new_add_actions)
            self.log_lfs.mkdir()
        elif delta_log.WriteMode.append == mode:
            dlog = delta_log.DeltaLogEntry.AppendTable(partition_by, new_add_actions)
        elif delta_log.WriteMode.overwrite == mode:
            existing_add_actions = delta_log.resolve_add_actions(versioned_log_entries).values()
            dlog = delta_log.DeltaLogEntry.OverwriteTable(partition_by, existing_add_actions, new_add_actions)

        with storage.open(self.log_lfs.append_path(f"{new_table_version:020}.json"), "w") as fh:
            dlog.write(fh)

class DeltaTable:
    def __init__(
        self,
        loc: str | storage.Location | storage.LocatedFS,
        log_loc: str | storage.Location | storage.LocatedFS | None = None,
        storage_options: dict | None = None,
    ):
        self.lfs = storage.LocatedFS.resolve(loc, storage_options)
        if log_loc is None:
            self.log_lfs = self.lfs.append_path("_delta_log")
        else:
            self.log_lfs = storage.LocatedFS.resolve(log_loc, storage_options)
        self.log = read_versioned_log_entries(self.log_lfs)
        self.adds = delta_log.resolve_add_actions(self.log)

    @property
    def version(self) -> int:
        if not self.log:
            return -1
        return max(self.log.keys())

    def to_pyarrow_dataset(self):
        paths = [self.lfs.append_path(path).path for path in self.adds]
        return pyarrow.dataset.dataset(
            paths,
            format="parquet",
            partitioning="hive",
            filesystem=self.lfs.fs,
        )
