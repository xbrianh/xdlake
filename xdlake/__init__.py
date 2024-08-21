import functools
import operator
from uuid import uuid4
from collections import defaultdict
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset
import pyarrow.parquet

from xdlake import delta_log, dataset_utils, storage, utils


def read_delta_log(
    loc: str | storage.Location,
    version: int | None = None,
    storage_options: dict | None = None,
) -> delta_log.DeltaLog:
    loc = storage.Location.with_location(loc, storage_options=storage_options)
    dlog = delta_log.DeltaLog()
    if not loc.exists():
        return dlog
    for entry_loc in loc.list_files_sorted():
        entry_version = int(entry_loc.basename().split(".")[0])
        with entry_loc.open() as fh:
            dlog[entry_version] = delta_log.DeltaLogEntry.with_handle(fh)
        if version in dlog:
            break
    return dlog

class Writer:
    def __init__(
        self,
        loc: str | storage.Location,
        log_loc: str | storage.Location | None = None,
        mode: str | delta_log.WriteMode = delta_log.WriteMode.append.name,
        schema_mode: str = "overwrite",
        partition_by: list | None = None,
        storage_options: dict | None = None,
    ):
        self.loc = storage.Location.with_location(loc, storage_options=storage_options)
        if log_loc is None:
            self.log_loc = self.loc.append_path("_delta_log")
        else:
            self.log_loc = storage.Location.with_location(log_loc, storage_options=storage_options)
        self.mode = delta_log.WriteMode[mode] if isinstance(mode, str) else mode
        self.schema_mode = schema_mode
        self.partition_by = partition_by or list()
        self.dlog = read_delta_log(self.log_loc)
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
        existing_partition_columns = self.dlog.partition_columns()
        if new_partition_by is None:
            pass
        elif set(existing_partition_columns) != set(new_partition_by):
            raise ValueError(f"Expected partition columns {existing_partition_columns}, got {self.partition_by}")
        return existing_partition_columns

    @classmethod
    def write(
        cls,
        loc: str | storage.Location,
        data: pa.Table | pa.dataset.Dataset | pa.RecordBatch,
        write_arrow_dataset_options: dict | None = None,
        **kwargs,
    ) -> int | None:
        writer = cls(loc, **kwargs)
        if writer._error_and_ignore:
            return None
        ds = dataset_utils.union_dataset(data)
        schema = writer.evaluate_schema(ds.schema)
        new_add_actions = writer.write_data(ds, write_arrow_dataset_options)
        return writer.write_deltalog_entry(schema, new_add_actions)

    def _add_actions_for_foreign_dataset(
        self,
        ds: pa.dataset.FileSystemDataset,
    ) -> list[delta_log.Add]:
        # TODO: is there a better way to find protocl? this looks potentially flaky
        scheme_map = {
            "py::fsspec+('s3', 's3a')": "s3://",
            "py::fsspec+('gs', 'gcs')": "gs://",
            "local": "file://",
        }

        add_actions = list()
        for fragment in ds.get_fragments():
            md = pa.parquet.ParquetFile(fragment.path, filesystem=ds.filesystem).metadata
            stats = delta_log.Statistics.from_parquet_file_metadata(md)
            url = f"{scheme_map[ds.filesystem.type_name]}{fragment.path}"
            info = ds.filesystem.get_file_info(fragment.path)
            partition_values = pyarrow.dataset.get_partition_keys(fragment.partition_expression)
            add_actions.append(
                delta_log.Add(
                    path=url,
                    modificationTime=utils.timestamp(),
                    size=info.size,
                    stats=stats.json(),
                    partitionValues=partition_values,
                )
            )

        return add_actions

    @classmethod
    def import_refs(
        cls,
        loc: str | storage.Location,
        refs: str | Iterable[str] | storage.Location | pa.Table | pa.RecordBatch | pa.dataset.Dataset,
        **kwargs,
    ) -> int | None:
        writer = cls(loc, **kwargs)
        if writer._error_and_ignore:
            return None
        ds = dataset_utils.union_dataset(refs)
        schema = writer.evaluate_schema(ds.schema)
        new_add_actions = list()
        for child_ds in ds.children:
            new_add_actions.extend(writer._add_actions_for_foreign_dataset(child_ds))
        return writer.write_deltalog_entry(schema, new_add_actions)

    def evaluate_schema(self, pyarrow_schema: pa.Schema) -> delta_log.Schema:
        schema = delta_log.Schema.from_pyarrow_schema(pyarrow_schema)
        if self.dlog.version is None:
            return schema
        else:
            existing_schema = self.dlog.schema()
            if delta_log.WriteMode.append == self.mode:
                if "merge" == self.schema_mode:
                    schema = existing_schema.merge(schema)
                elif existing_schema != schema:
                    raise ValueError("Schema mismatch")
            return schema

    def write_deltalog_entry(self, schema: delta_log.Schema, add_actions: list[delta_log.Add]) -> int:
        new_entry = delta_log.DeltaLogEntry()
        if 0 == self.version_to_write:
            new_entry = delta_log.DeltaLogEntry.CreateTable(self.log_loc.path, schema, self.partition_by, add_actions)
            self.log_loc.mkdir()
        elif delta_log.WriteMode.append == self.mode:
            new_entry = delta_log.DeltaLogEntry.AppendTable(self.partition_by, add_actions, schema)
        elif delta_log.WriteMode.overwrite == self.mode:
            existing_add_actions = self.dlog.add_actions().values()
            new_entry = delta_log.DeltaLogEntry.OverwriteTable(self.partition_by, existing_add_actions, add_actions)

        with self.log_loc.append_path(f"{self.version_to_write:020}.json").open(mode="w") as fh:
            new_entry.write(fh)

        return self.version_to_write

    def write_data(self, ds: pa.dataset.Dataset, write_arrow_dataset_options: dict | None = None) -> list[delta_log.Add]:
        add_actions = list()

        def visitor(visited_file):
            stats = delta_log.Statistics.from_parquet_file_metadata(
                pa.parquet.ParquetFile(visited_file.path).metadata
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

        write_arrow_dataset_options = write_arrow_dataset_options or dict()
        if self.partition_by is not None:
            write_arrow_dataset_options["partitioning"] = self.partition_by
            write_arrow_dataset_options["partitioning_flavor"] = "hive"

        pa.dataset.write_dataset(
            ds,
            self.loc.path,
            format="parquet",
            filesystem=self.loc.fs,
            basename_template=f"{self.version_to_write}-{uuid4()}-{{i}}.parquet",
            file_visitor=visitor,
            existing_data_behavior="overwrite_or_ignore",
            ** write_arrow_dataset_options,
        )

        return add_actions

class DeltaTable:
    def __init__(
        self,
        loc: str | storage.Location,
        log_loc: str | storage.Location | None = None,
        version: int | None = None,
        storage_options: dict | None = None,
    ):
        self.loc = storage.Location.with_location(loc, storage_options=storage_options)
        if log_loc is None:
            self.log_loc = self.loc.append_path("_delta_log")
        else:
            self.log_loc = storage.Location.with_location(log_loc, storage_options=storage_options)
        self.dlog = read_delta_log(self.log_loc, version=version)
        self.adds = self.dlog.add_actions()
        self.partition_columns = self.dlog.partition_columns()
        self.pyarrow_file_format = pyarrow.dataset.ParquetFileFormat(
            default_fragment_scan_options=pyarrow.dataset.ParquetFragmentScanOptions(pre_buffer=True)
        )

    @property
    def version(self) -> int:
        if not self.dlog:
            return -1
        return max(self.dlog.entries.keys())

    def add_action_to_fragment(self, add: delta_log.Add, filesystem: pa.fs.PyFileSystem) -> pa.dataset.Fragment:
        if self.partition_columns:
            partition_expressions = [
                pc.equal(pc.field(name), pc.scalar(value))
                for name, value in add.partitionValues.items()
            ]
            partition_expression = functools.reduce(operator.and_, partition_expressions)
        else:
            partition_expression = None

        # TODO add min/max and other info to partition expression to help pyarrow do filtering

        fragment = self.pyarrow_file_format.make_fragment(
            add.path,
            partition_expression=partition_expression,
            filesystem=filesystem,
        )

        return fragment

    def resolve_adds(self) -> dict[str, list[delta_log.Add]]:
        filesystems = defaultdict(list)
        for path, add in self.adds.items():
            is_absolute = "://" in path
            if is_absolute:
                loc = storage.Location.with_location(path)
            else:
                loc = self.loc.append_path(path)
                add.path = loc.path
            filesystems[storage.get_filesystem(loc.url)].append(add)
        return dict(filesystems)

    def to_pyarrow_dataset(self) -> pyarrow.dataset.Dataset:
        datasets = list()
        for fs, adds in self.resolve_adds().items():
            pyfs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fs))
            fragments = [
                self.add_action_to_fragment(add, pyfs)
                for add in adds
            ]
            ds = pyarrow.dataset.FileSystemDataset(
                fragments,
                self.dlog.schema().to_pyarrow_schema(),
                self.pyarrow_file_format,
                pyfs,
            )
            datasets.append(ds)
        return pa.dataset.dataset(datasets)
