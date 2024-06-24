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
        storage_options: dict | None = None,
    ):
        self.so = storage.StorageObject.resolve(loc, storage_options)
        if log_loc is None:
            self.log_so = self.so.append_path("_delta_log")
        else:
            self.log_so = storage.StorageObject.resolve(log_loc, storage_options)

    def write_data(self, ds: pa.dataset.Dataset, version: int, **write_kwargs) -> list[delta_log.Add]:
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

        pa.dataset.write_dataset(
            ds,
            self.so.path,
            format="parquet",
            filesystem=self.so.fs,
            basename_template=f"{version}-{uuid4()}-{{i}}.parquet",
            file_visitor=visitor,
            existing_data_behavior="overwrite_or_ignore",
            ** write_kwargs,
        )

        return add_actions

    def write(
        self,
        data: pa.Table | pa.dataset.Dataset | pa.RecordBatch,
        mode: str | delta_log.WriteMode = delta_log.WriteMode.append.name,
        schema_mode: str = "overwrite",
        partition_by: list | None = None,
        storage_options: dict | None = None,
    ):
        # TODO refactor this method, shit's getting complicated

        mode = delta_log.WriteMode[mode] if isinstance(mode, str) else mode

        ds = dataset.resolve(data)
        schema = delta_log.Schema.from_pyarrow_schema(ds.schema)
        merged_schema: delta_log.Schema | None = None

        dlog = read_delta_log(self.log_so)
        if not dlog.entries:
            new_table_version = 0
        else:
            new_table_version = 1 + max(dlog.entries.keys())
            if delta_log.WriteMode.error == mode:
                raise FileExistsError(f"Table already exists at version {new_table_version - 1}")
            elif delta_log.WriteMode.ignore == mode:
                return
            existing_schema = dlog.resolve_schema()
            if delta_log.WriteMode.append == mode:
                if "merge" == schema_mode:
                    merged_schema = existing_schema.merge(schema)
                elif existing_schema != schema:
                    raise ValueError("Schema mismatch")

        write_kwargs: dict = dict()
        if partition_by is not None:
            write_kwargs["partitioning"] = partition_by
            write_kwargs["partitioning_flavor"] = "hive"
        else:
            partition_by = list()

        new_add_actions = self.write_data(ds, new_table_version, **write_kwargs)

        new_entry = delta_log.DeltaLogEntry()
        if 0 == new_table_version:
            new_entry = delta_log.DeltaLogEntry.CreateTable(self.log_so.path, schema, partition_by, new_add_actions)
            self.log_so.mkdir()
        elif delta_log.WriteMode.append == mode:
            new_entry = delta_log.DeltaLogEntry.AppendTable(partition_by, new_add_actions, merged_schema)
        elif delta_log.WriteMode.overwrite == mode:
            existing_add_actions = dlog.resolve_add_actions().values()
            new_entry = delta_log.DeltaLogEntry.OverwriteTable(partition_by, existing_add_actions, new_add_actions)

        with storage.open(self.log_so.append_path(f"{new_table_version:020}.json"), "w") as fh:
            new_entry.write(fh)

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
