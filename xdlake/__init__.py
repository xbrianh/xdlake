from uuid import uuid4

import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet

from xdlake import delta_log, dataset, storage, utils


def read_delta_log(
    loc: str | storage.Location | storage.StorageObject,
    version: int | None = None,
    storage_options: dict | None = None,
) -> delta_log.DeltaLog:
    so = storage.StorageObject.resolve(loc, storage_options)
    dlog = delta_log.DeltaLog()
    if not so.exists():
        return dlog
    for entry_lfs in so.list_files_sorted():
        entry_version = int(entry_lfs.loc.basename().split(".")[0])
        with storage.open(entry_lfs) as fh:
            dlog[entry_version] = delta_log.DeltaLogEntry(fh)
        if version in dlog:
            break
    return dlog

class Writer:
    def __init__(
        self,
        loc: str | storage.Location | storage.StorageObject,
        log_loc: str | storage.Location | storage.StorageObject | None = None,
        mode: str | delta_log.WriteMode = delta_log.WriteMode.append.name,
        schema_mode: str = "overwrite",
        partition_by: list | None = None,
        storage_options: dict | None = None,
    ):
        self.so = storage.StorageObject.resolve(loc, storage_options)
        if log_loc is None:
            self.log_so = self.so.append_path("_delta_log")
        else:
            self.log_so = storage.StorageObject.resolve(log_loc, storage_options)
        self.mode = delta_log.WriteMode[mode] if isinstance(mode, str) else mode
        self.schema_mode = schema_mode
        self.partition_by = partition_by or list()
        self.dlog = read_delta_log(self.log_so)
        self._error_and_ignore = False

        if self.dlog.version is None:
            self.version_to_write = 0
        else:
            self.version_to_write = 1 + self.dlog.version
            match self.mode:
                case delta_log.WriteMode.error:
                    raise FileExistsError(f"Table already exists at version {self.version_to_write - 1}")
                case delta_log.WriteMode.ignore:
                    self._error_and_ignore = True
                case _:
                    self.partition_by = self._resolve_partition_by(partition_by)

    def _resolve_partition_by(self, new_partition_by) -> list:
        existing_partition_columns = self.dlog.resolve_partition_columns()
        if new_partition_by is None:
            pass
        elif set(existing_partition_columns) != set(new_partition_by):
            raise ValueError(f"Expected partition columns {existing_partition_columns}, got {self.partition_by}")
        return existing_partition_columns

    @classmethod
    def write(
        cls,
        loc: str | storage.Location | storage.StorageObject,
        data: pa.Table | pa.dataset.Dataset | pa.RecordBatch,
        write_arrow_dataset_options: dict | None = None,
        **kwargs,
    ):
        writer = cls(loc, **kwargs)
        if writer._error_and_ignore:
            return
        ds = dataset.resolve(data)
        schema = writer.evaluate_schema(ds.schema)
        new_add_actions = writer.write_data(ds, write_arrow_dataset_options)
        writer.write_deltalog_entry(schema, new_add_actions)

    def evaluate_schema(self, pyarrow_schema: pa.Schema) -> delta_log.Schema:
        schema = delta_log.Schema.from_pyarrow_schema(pyarrow_schema)
        if self.dlog.version is None:
            return schema
        else:
            existing_schema = self.dlog.resolve_schema()
            if delta_log.WriteMode.append == self.mode:
                if "merge" == self.schema_mode:
                    schema = existing_schema.merge(schema)
                elif existing_schema != schema:
                    raise ValueError("Schema mismatch")
            return schema

    def write_deltalog_entry(self, schema: delta_log.Schema, add_actions: list[delta_log.Add]):
        new_entry = delta_log.DeltaLogEntry()
        if 0 == self.version_to_write:
            new_entry = delta_log.DeltaLogEntry.CreateTable(self.log_so.path, schema, self.partition_by, add_actions)
            self.log_so.mkdir()
        elif delta_log.WriteMode.append == self.mode:
            new_entry = delta_log.DeltaLogEntry.AppendTable(self.partition_by, add_actions, schema)
        elif delta_log.WriteMode.overwrite == self.mode:
            existing_add_actions = self.dlog.resolve_add_actions().values()
            new_entry = delta_log.DeltaLogEntry.OverwriteTable(self.partition_by, existing_add_actions, add_actions)

        with storage.open(self.log_so.append_path(f"{self.version_to_write:020}.json"), "w") as fh:
            new_entry.write(fh)

    def write_data(self, ds: pa.dataset.Dataset, write_arrow_dataset_options: dict | None = None) -> list[delta_log.Add]:
        add_actions = list()

        def visitor(visited_file):
            stats = delta_log.Statistics.from_parquet_file_metadata(
                pa.parquet.ParquetFile(visited_file.path).metadata
            )

            relpath = visited_file.path.replace(self.so.path, "").strip("/")
            partition_values = dict()

            for part in relpath.split("/"):
                if "=" in part:
                    key, value = part.split("=")
                    partition_values[key] = value

            add_actions.append(
                delta_log.Add(
                    path=relpath,
                    modificationTime=utils.timestamp(),
                    size=self.so.fs.size(visited_file.path),
                    stats=stats.json(),
                    partitionValues=partition_values
                )
            )

        write_arrow_dataset_options = write_arrow_dataset_options or dict()
        if self.partition_by is not None:
            write_arrow_dataset_options["partitioning"] = self.partition_by
            write_arrow_dataset_options["partitioning_flavor"] = "hive"

        pa.dataset.write_dataset(
            ds,
            self.so.path,
            format="parquet",
            filesystem=self.so.fs,
            basename_template=f"{self.version_to_write}-{uuid4()}-{{i}}.parquet",
            file_visitor=visitor,
            existing_data_behavior="overwrite_or_ignore",
            ** write_arrow_dataset_options,
        )

        return add_actions

class DeltaTable:
    def __init__(
        self,
        loc: str | storage.Location | storage.StorageObject,
        log_loc: str | storage.Location | storage.StorageObject | None = None,
        storage_options: dict | None = None,
    ):
        self.so = storage.StorageObject.resolve(loc, storage_options)
        if log_loc is None:
            self.log_so = self.so.append_path("_delta_log")
        else:
            self.log_so = storage.StorageObject.resolve(log_loc, storage_options)
        self.dlog = read_delta_log(self.log_so)
        self.adds = self.dlog.resolve_add_actions()

    @property
    def version(self) -> int:
        if not self.dlog:
            return -1
        return max(self.dlog.entries.keys())

    def to_pyarrow_dataset(self):
        paths = [self.so.append_path(path).path for path in self.adds]
        return pa.dataset.dataset(
            paths,
            format="parquet",
            partitioning="hive",
            filesystem=self.so.fs,
            schema=self.dlog.resolve_schema().to_pyarrow_schema(),
        )
