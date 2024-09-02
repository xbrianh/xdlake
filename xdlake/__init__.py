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
    if loc.exists():
        for entry_loc in loc.list_files_sorted():
            entry_version = int(entry_loc.basename().split(".")[0])
            with entry_loc.open() as fh:
                dlog[entry_version] = delta_log.DeltaLogEntry.with_handle(fh)
            if version in dlog:
                break
    return dlog


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
        if self.dlog.entries:
            self.adds = self.dlog.add_actions()
            self.partition_columns = self.dlog.partition_columns()
            self.pyarrow_file_format = pyarrow.dataset.ParquetFileFormat(
                default_fragment_scan_options=pyarrow.dataset.ParquetFragmentScanOptions(pre_buffer=True)
            )
            self._version_to_write = 1 + self.dlog.version
        else:
            self._version_to_write = 0

    @property
    def version(self) -> int:
        return self.dlog.version

    @property
    def versions(self) -> list[int]:
        return self.dlog.versions

    def load_as_version(self, version: int) -> "DeltaTable":
        return type(self)(self.loc, self.log_loc, version=version)

    def add_action_to_fragment(self, add: delta_log.Add) -> tuple[storage.Location, pa.dataset.Fragment]:
        loc = storage.absloc(add.path, self.loc)
        pyfs = dataset_utils.get_py_filesystem(loc.fs)
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
            loc.path,
            partition_expression=partition_expression,
            filesystem=pyfs,
        )

        return loc, fragment

    def get_fragments(self) -> dict[str, list[pyarrow.dataset.Fragment]]:
        fragments = defaultdict(list)
        for path, add in self.adds.items():
            loc, fragment = self.add_action_to_fragment(add)
            fragments[loc.fs].append(fragment)
        return dict(fragments)

    def to_pyarrow_dataset(self) -> pyarrow.dataset.Dataset:
        datasets = list()
        for fs, fragments in self.get_fragments().items():
            ds = pyarrow.dataset.FileSystemDataset(
                fragments,
                self.dlog.schema().to_pyarrow_schema(),
                self.pyarrow_file_format,
                dataset_utils.get_py_filesystem(fs),
            )
            datasets.append(ds)
        return pa.dataset.dataset(datasets)

    def to_pyarrow_table(self) -> pa.Table:
        """Return arrow table."""
        return self.to_pyarrow_dataset().to_table()

    def write(
        self,
        data: pa.Table | pa.dataset.Dataset | pa.RecordBatch,
        mode: str | delta_log.WriteMode = delta_log.WriteMode.append.name,
        schema_mode: str = "overwrite",
        partition_by: list | None = None,
        write_arrow_dataset_options: dict | None = None,
        storage_options: dict | None = None,
    ) -> "DeltaTable":
        mode = delta_log.WriteMode[mode] if isinstance(mode, str) else mode
        if self._version_to_write:
            match mode:
                case delta_log.WriteMode.error:
                    raise FileExistsError(f"Table already exists at version {self._version_to_write - 1}")
                case delta_log.WriteMode.ignore:
                    return self  # return the same table that called the write op
                case _:
                    partition_by = self.dlog.validate_partition_by(partition_by)
        ds = dataset_utils.union_dataset(data)
        schema = self.dlog.evaluate_schema(ds.schema, mode, schema_mode)
        new_add_actions = self.write_data(ds, partition_by, write_arrow_dataset_options)
        self.write_deltalog_entry(mode, schema, new_add_actions, partition_by)
        return type(self)(self.loc, self.log_loc)

    def import_refs(
        self,
        refs: str | Iterable[str] | storage.Location | pa.Table | pa.RecordBatch | pa.dataset.Dataset,
        mode: str | delta_log.WriteMode = delta_log.WriteMode.append.name,
        schema_mode: str = "overwrite",
        partition_by: list | None = None,
    ) -> "DeltaTable":
        mode = delta_log.WriteMode[mode] if isinstance(mode, str) else mode
        if self._version_to_write:
            match mode:
                case delta_log.WriteMode.error:
                    raise FileExistsError(f"Table already exists at version {self._version_to_write - 1}")
                case delta_log.WriteMode.ignore:
                    return self  # return the same table that called the write op
                case _:
                    partition_by = self.dlog.validate_partition_by(partition_by)
        ds = dataset_utils.union_dataset(refs)
        schema = self.dlog.evaluate_schema(ds.schema, mode, schema_mode)
        new_add_actions = list()
        for child_ds in ds.children:
            new_add_actions.extend(self.add_actions_for_foreign_dataset(child_ds))
        self.write_deltalog_entry(mode, schema, new_add_actions, partition_by)
        return type(self)(self.loc, self.log_loc)

    def clone(self, dst_loc: str | storage.Location, dst_log_loc: str | None = None):
        """Clone the DeltaTable

        The cloned table contains add actions that reference files in the source table without copying data. Version history is preserved.

        Args:
            dst_loc (str | Location): Location of the destination table.
            dst_log_loc (str | None): Location of the destination table's delta log if stored remotely.
        """
        dst_loc = storage.Location.with_location(dst_loc)
        dst_dlog_loc = storage.Location.with_location(dst_log_loc or dst_loc.append_path("_delta_log"))
        for version, src_entry in self.dlog.entries.items():
            dst_actions = list()
            for src_action in src_entry.actions:
                if isinstance(src_action, (delta_log.Add, delta_log.Remove)):
                    dst_action = src_action.replace(path=storage.absloc(src_action.path, self.loc).path)
                elif isinstance(src_action, delta_log.TableCommit):
                    dst_action = src_action
                else:
                    dst_action = src_action
                dst_actions.append(dst_action)
            with dst_dlog_loc.append_path(utils.filename_for_version(version)).open(mode="w") as fh:
                delta_log.DeltaLogEntry(dst_actions).write(fh)
        return type(self)(dst_loc, dst_log_loc)

    def delete(
        self,
        where: pc.Expression,
        write_arrow_dataset_options: dict | None = None,
    ):
        ds = self.to_pyarrow_dataset()
        batches_to_write = list()
        existing_add_actions = {
            storage.absloc(path, self.loc).path: add
            for path, add in self.dlog.add_actions().items()
        }
        adds_to_remove = dict()
        did_delete_rows: bool
        num_copied_rows = 0
        num_deleted_rows = 0
        for tb in ds.scanner().scan_batches():
            did_delete_rows = False
            try:
                rb_keep = tb.record_batch.filter(~where)
                if tb.record_batch.num_rows > rb_keep.num_rows:  # something is deleted!
                    did_delete_rows = True
                    batches_to_write.append(rb_keep)
                    num_copied_rows += rb_keep.num_rows
                    num_deleted_rows += (tb.record_batch.num_rows - rb_keep.num_rows)
            except IndexError:
                # entire record batch was deleted - is this a pyarrow bug?
                num_deleted_rows += tb.record_batch.num_rows
                did_delete_rows = True
            if did_delete_rows:
                adds_to_remove[tb.fragment.path] = existing_add_actions[tb.fragment.path]

        new_add_actions = self.write_data(
            pyarrow.dataset.dataset(batches_to_write, schema=self.dlog.schema().to_pyarrow_schema()),
            self.partition_columns,
            write_arrow_dataset_options,
        )
        new_entry = delta_log.DeltaLogEntry.DeleteTable(
            predicate="pyarrow expression",
            add_actions_to_remove=adds_to_remove.values(),
            add_actions=new_add_actions,
            read_version=self.version,
            num_copied_rows=num_copied_rows,
            num_deleted_rows=num_deleted_rows,
        )
        with self.log_loc.append_path(utils.filename_for_version(self._version_to_write)).open(mode="w") as fh:
            new_entry.write(fh)
        return type(self)(self.loc, self.log_loc)

    def write_data(
        self,
        ds: pa.dataset.Dataset,
        partition_by: list | None = None,
        write_arrow_dataset_options: dict | None = None,
    ) -> list[delta_log.Add]:
        add_actions = list()

        def visitor(visited_file):
            stats = delta_log.Statistics.from_parquet_file_metadata(
                pa.parquet.ParquetFile(visited_file.path, filesystem=self.loc.fs).metadata
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
        if partition_by is not None:
            write_arrow_dataset_options["partitioning"] = partition_by
            write_arrow_dataset_options["partitioning_flavor"] = "hive"

        pa.dataset.write_dataset(
            ds,
            self.loc.path,
            format="parquet",
            filesystem=self.loc.fs,
            basename_template=f"{self._version_to_write}-{uuid4()}-{{i}}.parquet",
            file_visitor=visitor,
            existing_data_behavior="overwrite_or_ignore",
            ** write_arrow_dataset_options,
        )

        return add_actions

    def add_actions_for_foreign_dataset(self, ds: pa.dataset.FileSystemDataset) -> list[delta_log.Add]:
        add_actions = list()
        for fragment in ds.get_fragments():
            md = pa.parquet.ParquetFile(fragment.path, filesystem=ds.filesystem).metadata
            stats = delta_log.Statistics.from_parquet_file_metadata(md)
            info = ds.filesystem.get_file_info(fragment.path)
            partition_values = pyarrow.dataset.get_partition_keys(fragment.partition_expression)
            add_actions.append(
                delta_log.Add(
                    path=fragment.path,
                    modificationTime=utils.timestamp(),
                    size=info.size,
                    stats=stats.json(),
                    partitionValues=partition_values,
                )
            )

        return add_actions

    def write_deltalog_entry(
        self,
        mode,
        schema: delta_log.Schema,
        add_actions: list[delta_log.Add],
        partition_by: list | None = None,
    ):
        partition_by = partition_by or list()
        new_entry = delta_log.DeltaLogEntry()
        if 0 == self._version_to_write:
            new_entry = delta_log.DeltaLogEntry.CreateTable(self.log_loc.path, schema, partition_by, add_actions)
            self.log_loc.mkdir()
        elif delta_log.WriteMode.append == mode:
            new_entry = delta_log.DeltaLogEntry.AppendTable(partition_by, add_actions, schema)
        elif delta_log.WriteMode.overwrite == mode:
            existing_add_actions = self.dlog.add_actions().values()
            new_entry = delta_log.DeltaLogEntry.OverwriteTable(partition_by, existing_add_actions, add_actions)

        with self.log_loc.append_path(utils.filename_for_version(self._version_to_write)).open(mode="w") as fh:
            new_entry.write(fh)
