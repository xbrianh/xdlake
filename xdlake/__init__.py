import functools
import operator
from uuid import uuid4
from contextlib import contextmanager
from collections import defaultdict
from typing import Generator, Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset
import pyarrow.parquet

from xdlake import delta_log, dataset_utils, storage, utils


class DeltaTable:
    """A DeltaTable is a high-level API for working with Delta Lake tables.

    This class defines the read and write operations that can be performed on a delta table. If you don't like how it
    works you can subclass it: it's meant to be customizable and extensible. If you _still_ hate it, submit a
    pull request.

    Args:
        loc (str | Location): Root of the table directory.
        log_loc (str | Location, optional): Root of the transaction log directory. This is for remotely stored logs.
        version (int, optional): Read table at this version.
        storage_options (dict, optional): keyword arguments to pass to fsspec.filesystem

    Returns:
        DeltaTable: A new DeltaTable instance.
    """
    def __init__(
        self,
        loc: str | storage.Location,
        location_or_log: str | storage.Location | delta_log.DeltaLog | None = None,
        version: int | None = None,
        storage_options: dict | None = None,
    ):
        self.loc = storage.Location.with_location(loc, storage_options=storage_options)
        match location_or_log:
            case delta_log.DeltaLog():
                self.dlog = location_or_log
            case None | str() | storage.Location():
                if location_or_log is None:
                    log_loc = self.loc.append_path("_delta_log")
                else:
                    log_loc = storage.Location.with_location(location_or_log, storage_options=storage_options)
                self.dlog = delta_log.DeltaLog.with_location(log_loc, version=version)
            case _:
                raise TypeError(f"Unexpected type for 'location_or_log': {type(location_or_log)}")
        if self.dlog.entries:
            self.adds = self.dlog.add_actions()
            self.partition_columns = self.dlog.partition_columns()
            self.pyarrow_file_format = pyarrow.dataset.ParquetFileFormat(
                default_fragment_scan_options=pyarrow.dataset.ParquetFragmentScanOptions(pre_buffer=True)
            )
            self._version_to_write = 1 + self.dlog.version
        else:
            self._version_to_write = 0

    def version(self) -> int:
        """Return the version of the table."""
        return self.dlog.version

    @property
    def versions(self) -> list[int]:
        """Return the versions of the table."""
        return self.dlog.versions

    def history(self, reverse: bool=True) -> Generator[dict, None, None]:
        yield from self.dlog.history(reverse=reverse)

    def load_as_version(self, version: int) -> "DeltaTable":
        """Load the table at a specific version.

        Args:
            version (int): Version to load.

        Returns:
            DeltaTable: A new DeltaTable instance.
        """
        return type(self)(self.loc, self.dlog.loc, version=version)

    def add_action_to_fragment(self, add: delta_log.actions.Add) -> tuple[storage.Location, pa.dataset.Fragment]:
        """Convert a delta log add action to a pyarrow dataset fragment.

        Args:
            add (delta_log.actions.Add): Add action.

        Returns:
            tuple[storage.Location, pa.dataset.Fragment]
        """
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
        """Return a dictionary of fragments by filesystem."""
        fragments = defaultdict(list)
        for path, add in self.adds.items():
            loc, fragment = self.add_action_to_fragment(add)
            fragments[loc.fs].append(fragment)
        return dict(fragments)

    def file_uris(self) -> list[str]:
        """Return the URI for each file in the current version of the table."""
        return [storage.absloc(add.path, self.loc).url
                for add in self.adds.values()]

    def to_pyarrow_dataset(self) -> pyarrow.dataset.Dataset:
        """Return arrow dataset."""
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

    def to_pandas(self):
        """Return pandas DataFrame."""
        return self.to_pyarrow_table().to_pandas()

    def write(
        self,
        data: pa.Table | pa.dataset.Dataset | pa.RecordBatch,
        mode: str | delta_log.WriteMode = delta_log.WriteMode.append.name,
        schema_mode: str = "overwrite",
        partition_by: list | None = None,
        write_arrow_dataset_options: dict | None = None,
        custom_metadata: dict | None = None,
        storage_options: dict | None = None,
    ) -> "DeltaTable":
        """Write data to the table.

        Args:
            data (Table | Dataset | RecordBatch | pandas.DataFrame): Data to write. May be an iterable of accepted types.
            mode (str | WriteMode, optional): Write mode. Must be one of "append", "overwrite", "error", or "ignore".
            schema_mode (str, optional): Schema mode.
            partition_by (list[str], optional): Partition columns.
            write_arrow_dataset_options (dict, optional): Options to pass to pyarrow.dataset.write_dataset.
            storage_options (dict, optional): Options to pass to fsspec.filesystem.

        Returns:
            DeltaTable: A new DeltaTable instance.
        """
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
        entry = self.dlog.entry_for_write_mode(mode, schema, new_add_actions, partition_by)
        entry.add_extra_commit_info(custom_metadata)
        return self.commit(entry)

    def import_refs(
        self,
        refs: str | Iterable[str] | storage.Location | pa.dataset.Dataset,
        mode: str | delta_log.WriteMode = delta_log.WriteMode.append.name,
        schema_mode: str = "overwrite",
        partition_by: list | None = None,
        custom_metadata: dict | None = None,
    ) -> "DeltaTable":
        """Import data from a foreign dataset.

        Args:
            refs (str | Iterable[str] | Location | Table | RecordBatch | Dataset): Foreign dataset.
            mode (str | WriteMode, optional): Write mode. Must be one of "append", "overwrite", "error", or "ignore".
            schema_mode (str, optional): Schema mode.
            partition_by (list[str], optional): Partition columns.



        Returns:
            DeltaTable: A new DeltaTable instance.
        """
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
        entry = self.dlog.entry_for_write_mode(mode, schema, new_add_actions, partition_by)
        entry.add_extra_commit_info(custom_metadata)
        return self.commit(entry)

    def clone(self, dst_loc: str | storage.Location, dst_log_loc: str | None = None) -> "DeltaTable":
        """Clone the DeltaTable

        The cloned table contains add actions that reference files in the source table without copying data. Version history is preserved.

        Args:
            dst_loc (str | Location): Location of the destination table.
            dst_log_loc (str | None): Location of the destination table's delta log if stored remotely.

        Returns:
            DeltaTable: A new DeltaTable instance.
        """
        dst_loc = storage.Location.with_location(dst_loc)
        dst_dlog_loc = storage.Location.with_location(dst_log_loc or dst_loc.append_path("_delta_log"))
        for version, src_entry in self.dlog.entries.items():
            dst_actions = list()
            for src_action in src_entry.actions:
                if isinstance(src_action, (delta_log.actions.Add, delta_log.actions.Remove)):
                    dst_action = src_action.replace(path=storage.absloc(src_action.path, self.loc).path)
                elif isinstance(src_action, delta_log.actions.TableCommit):
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
        custom_metadata: dict | None = None,
    ) -> "DeltaTable":
        """Delete rows from the table.

        Args:
            where (Expression): PyArrow expression.
            write_arrow_dataset_options (dict, optional): Options to pass to pyarrow.dataset.write_dataset.

        Returns:
            DeltaTable: A new DeltaTable instance.
        """
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
        new_entry = delta_log.DeltaLogEntry.delete_table(
            predicate="pyarrow expression",
            add_actions_to_remove=adds_to_remove.values(),
            add_actions=new_add_actions,
            read_version=self.version(),
            num_copied_rows=num_copied_rows,
            num_deleted_rows=num_deleted_rows,
        )
        new_entry.add_extra_commit_info(custom_metadata)
        return self.commit(new_entry)

    def restore(self, restore_version: int, custom_metadata: dict | None = None) -> "DeltaTable":
        prev_dlog = self.dlog.load_as_version(restore_version)
        prev_schema = prev_dlog.schema()
        prev_add_actions = prev_dlog.add_actions()
        curr_add_actions = self.dlog.add_actions()
        restore_add_actions = [prev_add_actions[p] for p in prev_add_actions if p not in curr_add_actions]
        add_actions_to_remove = [curr_add_actions[p] for p in curr_add_actions if p not in prev_add_actions]
        new_entry = delta_log.DeltaLogEntry.restore_table(
            read_version=self.version(),
            restore_version=restore_version,
            restore_schema=prev_schema,
            restore_partition_by=prev_dlog.partition_columns(),
            add_actions=restore_add_actions,
            add_actions_to_remove=add_actions_to_remove,
        )
        new_entry.add_extra_commit_info(custom_metadata)
        return self.commit(new_entry)

    def write_data(
        self,
        ds: pa.dataset.Dataset,
        partition_by: list | None = None,
        write_arrow_dataset_options: dict | None = None,
    ) -> list[delta_log.actions.Add]:
        """Write data and generate add actions for written files.

        This is used during table writes. If you want to change write behavior before the log is updated, this is where
        you do it.

        Args:
            ds (Dataset): Arrow dataset.
            partition_by (list[str], optional): Partition columns.
            write_arrow_dataset_options (dict, optional): Options to pass to pyarrow.dataset.write_dataset.

        Returns:
            list[delta_log.actions.Add]
        """

        add_actions = list()

        def visitor(visited_file):
            stats = delta_log.statistics.Statistics.from_parquet_file_metadata(
                pa.parquet.ParquetFile(visited_file.path, filesystem=self.loc.fs).metadata
            )

            relpath = visited_file.path.replace(self.loc.path, "").strip("/")
            partition_values = dict()

            for part in relpath.split("/"):
                if "=" in part:
                    key, value = part.split("=")
                    partition_values[key] = value

            add_actions.append(
                delta_log.actions.Add(
                    path=relpath,
                    modificationTime=utils.timestamp(),
                    size=self.loc.fs.size(visited_file.path),
                    stats=stats,
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

    def add_actions_for_foreign_dataset(self, ds: pa.dataset.FileSystemDataset) -> list[delta_log.actions.Add]:
        """Generate add actions for a foreign dataset.

        Args:
            ds (FileSystemDataset): Foreign dataset.

        Returns:
            list[delta_log.actions.Add]
        """
        add_actions = list()
        for fragment in ds.get_fragments():
            md = pa.parquet.ParquetFile(fragment.path, filesystem=ds.filesystem).metadata
            stats = delta_log.statistics.Statistics.from_parquet_file_metadata(md)
            info = ds.filesystem.get_file_info(fragment.path)
            partition_values = pyarrow.dataset.get_partition_keys(fragment.partition_expression)
            add_actions.append(
                delta_log.actions.Add(
                    path=fragment.path,
                    modificationTime=utils.timestamp(),
                    size=info.size,
                    stats=stats,
                    partitionValues=partition_values,
                )
            )

        return add_actions

    @contextmanager
    def commit_context(self, loc: storage.Location):
        """Context for transaction log writes.

        Locking can be implimented by overriding this method. The default behavior is to raise if loc exists.

        Args:
            loc (storage.Location): The location where the new transaction log etnry will be written.

        Returns:
            A context manager.
        """
        try:
            if loc.exists():
                raise FileExistsError("This transaction log version already exists!")
            yield
        finally:
            pass

    def commit(self, entry: delta_log.DeltaLogEntry) -> "DeltaTable":
        new_dlog = self.dlog.commit(entry, self.commit_context)
        return type(self)(self.loc, new_dlog)
