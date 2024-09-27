import re
import json
from enum import Enum
from contextlib import nullcontext, suppress
from collections.abc import ValuesView
from typing import IO, Generator, Iterable, Sequence

import pyarrow as pa

from xdlake import storage, utils
from xdlake.delta_log import actions
from xdlake.delta_log.schema import Schema


class WriteMode(Enum):
    append = "Append"
    overwrite = "Overwrite"
    error = "Error"
    ignore = "Ignore"

class DeltaLogEntry:
    """A single entry in the delta table transaction log.

    Args:
        actions (list, optional): The actions in this entry.
    """
    def __init__(self, actions: list | None = None):
        self.actions = actions or list()

    @classmethod
    def with_handle(cls, handle: IO):
        """Create a DeltaLogEntry from a file handle.

        Args:
            handle (IO): A file handle

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        actions = [cls.load_action(line) for line in handle]
        return cls(actions)

    @staticmethod
    def load_action(obj: str | bytes | dict) -> actions.DeltaLogAction:
        """Load an action from a string, bytes, or dict.

        Args:
            obj (str | bytes | dict): The JSON data to load.

        Returns:
            A new action object

        Raises:
            ValueError: If the action cannot be loaded.
        """
        if isinstance(obj, (str, bytes)):
            info = json.loads(obj)
        else:
            info = obj
        action_name = list(info.keys())[0]
        info = info[action_name]
        try:
            action_cls = actions.DeltaLogActionMeta.registered_actions[action_name]
            return action_cls.with_info(info)
        except Exception as e:
            raise ValueError(f"Cannot handle delta log action '{action_name}'") from e

    def write(self, handle):
        """Write the entry to a file handle.

        Args:
            handle (IO): A file handle
        """
        handle.write("\n".join([json.dumps(a.to_action_dict(), cls=utils._JSONEncoder) for a in self.actions]))

    def add_actions(self) -> list[actions.Add]:
        """Get all add actions in the entry."""
        return [a for a in self.actions
                if isinstance(a, actions.Add)]

    def remove_actions(self) -> list[actions.Remove]:
        """Get all remove actions in the entry."""
        return [a for a in self.actions
                if isinstance(a, actions.Remove)]

    def partition_columns(self) -> list[str] | None:
        """Get the partition columns in the entry."""
        for a in self.actions:
            if isinstance(a, actions.TableCommit):
                if a.operation == actions.TableCommitOperation.WRITE:
                    partition_by = a.operationParameters.get("partitionBy")
                    # deltalake writes a string sometimes?
                    if isinstance(partition_by, str):
                        partition_by = json.loads(partition_by)
                    return partition_by
                elif a.operation == actions.TableCommitOperation.CREATE:
                    return a.metadata["partitionColumns"]
        return None

    @classmethod
    def with_actions(cls, actions: list[actions.DeltaLogAction]) -> "DeltaLogEntry":
        """Create a new DeltaLogEntry with a list of actions.

        Args:
            actions (list): A list of actions

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        entry = cls()
        entry.actions.extend(actions)
        return entry

    @classmethod
    def create_table(cls, path: str, schema: Schema, partition_by: list, add_actions: list[actions.Add]) -> "DeltaLogEntry":
        """Create a new DeltaLogEntry for creating a table.

        Args:
            path (str): The path to
            schema (Schema): The schema of the table
            partition_by (list): The partition columns
            add_actions (list[Add]): The add actions

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        protocol = actions.Protocol()
        table_metadata = actions.TableMetadata(schemaString=schema, partitionColumns=partition_by)
        commit = actions.TableCommit.create(path, utils.timestamp(), table_metadata, protocol)
        return cls.with_actions([protocol, table_metadata, *add_actions, commit])

    @classmethod
    def append_table(cls, partition_by: list, add_actions: list[actions.Add], schema: Schema | None = None) -> "DeltaLogEntry":
        """Create a new DeltaLogEntry for appending to a table.

        Args:
            partition_by (list): The partition columns
            add_actions (list[Add]): The add actions
            schema (Schema, optional): The schema of the table.

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        commit = actions.TableCommit.write(utils.timestamp(), mode=WriteMode.append.value, partition_by=partition_by)
        the_actions = add_actions + [commit]
        if schema is not None:
            table_metadata = actions.TableMetadata(schemaString=schema, partitionColumns=partition_by)
            the_actions = [table_metadata] + the_actions
        return cls.with_actions(the_actions)

    @classmethod
    def overwrite_table(
        cls,
        partition_by: list,
        existing_add_actions: Iterable[actions.Add],
        add_actions: list[actions.Add]
    ) -> "DeltaLogEntry":
        """Create a new DeltaLogEntry for overwriting a table.

        Args:
            partition_by (list): The partition columns
            existing_add_actions (Iterable[actions.Add]): The existing add actions
            add_actions (list[actions.Add]): The new add actions

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        commit = actions.TableCommit.write(utils.timestamp(), mode=WriteMode.overwrite.value, partition_by=partition_by)
        remove_actions = actions.generate_remove_acctions(existing_add_actions)
        return cls.with_actions([*remove_actions, *add_actions, commit])

    @classmethod
    def delete_table(
        cls,
        *,
        predicate: str,
        add_actions_to_remove: Sequence[actions.Add] | ValuesView[actions.Add],
        add_actions: Sequence[actions.Add] | ValuesView[actions.Add],
        read_version: int,
        num_copied_rows: int,
        num_deleted_rows: int,
    ) -> "DeltaLogEntry":
        """Create a new DeltaLogEntry for deleting rows from a table.

        Args:
            predicate (str): The predicate used to delete rows.
            add_actions_to_remove (Sequence[actions.Add]): The add actions to remove.
            add_actions (Sequence[actions.Add]): The new add actions.
            read_version (int): The input version for the delete operation.
            num_copied_rows (int): The number of rows copied from the input version.
            num_deleted_rows (int): The number of rows deleted from the input version.

        Returns:
            DeltaLogEntry: A new DeltaLogEntry object
        """
        operation_metrics = {
            "num_added_files": len(add_actions),
            "num_removed_files": len(add_actions_to_remove),
            "num_copied_rows": num_copied_rows,
            "num_deleted_rows": num_deleted_rows,
        }
        commit = actions.TableCommit.delete(utils.timestamp(), predicate, read_version, operation_metrics)
        remove_actions = actions.generate_remove_acctions(add_actions_to_remove)
        return cls.with_actions([*remove_actions, *add_actions, commit])

    @classmethod
    def restore_table(
        cls,
        *,
        read_version: int,
        restore_version: int,
        restore_schema: Schema,
        restore_partition_by: list[str],
        add_actions_to_remove: Sequence[actions.Add] | ValuesView[actions.Add],
        add_actions: Sequence[actions.Add] | ValuesView[actions.Add],
    ) -> "DeltaLogEntry":
        operation_metrics = {
            "num_removed_files": len(add_actions_to_remove),
            "num_restored_files": len(add_actions),
        }
        commit = actions.TableCommit.restore(utils.timestamp(), read_version, restore_version, operation_metrics)
        remove_actions = actions.generate_remove_acctions(add_actions_to_remove)
        table_metadata = actions.TableMetadata(schemaString=restore_schema, partitionColumns=restore_partition_by)
        return cls.with_actions([table_metadata, *remove_actions, *add_actions, commit])

    def add_extra_commit_info(self, info: dict | None = None):
        if info is not None:
            for a in self.actions:
                if isinstance(a, actions.TableCommit):
                    a.extra_info.update(info)

class DeltaLog:
    """The transaction log of a delta table."""

    _log_entry_filename_re = re.compile("^\d+\.json$")

    def __init__(self, location: storage.Location):
        self.entries: dict[int, DeltaLogEntry] = dict()
        self.loc = location

    def __setitem__(self, key, val):
        self.entries[key] = val

    def __getitem__(self, key):
        return self.entries[key]

    def __contains__(self, key):
        return key in self.entries

    @classmethod
    def with_location(
        cls,
        loc: str | storage.Location,
        version: int | None = None,
        storage_options: dict | None = None,
    ) -> "DeltaLog":
        """Read a delta table transaction log.

        Args:
            loc (str | Location): Root of the transaction log directory.
            version (int, otional): Read log entries up to this version.
            storage_options (dict, optional): keyword arguments to pass to fsspec.filesystem

        Returns:
            delta_log.DeltaLog
        """
        dlog = cls(storage.Location.with_location(loc, storage_options=storage_options))
        if dlog.loc.exists():
            for entry_loc in dlog.loc.list_files_sorted():
                filename = entry_loc.basename()
                if cls._log_entry_filename_re.match(filename):
                    entry_version = int(filename.split(".", 1)[0])
                    with entry_loc.open() as fh:
                        dlog[entry_version] = DeltaLogEntry.with_handle(fh)
                    if version in dlog:
                        break
        return dlog

    def load_as_version(self, version: int | None = None):
        """Load the delta log up to a specific version.

        Args:
            version (int, optional): The version to load up to.

        Returns:
            DeltaLog: A new DeltaLog object
        """
        if version in self.versions:
            dlog = type(self)(self.loc)
            for v in self.versions:
                if v <= version:
                    dlog[v] = self[v]
                else:
                    break
            return dlog
        else:
            return type(self).with_location(self.loc)

    @property
    def version(self) -> int:
        """The largest version in the log."""
        return self.versions[-1]

    @property
    def versions(self) -> list[int]:
        """All versions in the log."""
        if self.entries:
            return sorted(self.entries.keys())
        else:
            raise ValueError("This delta log is empty!")

    def history(self, reverse: bool=True) -> Generator[dict, None, None]:
        for version in reversed(self.versions) if reverse else self.versions:
            for action in self[version].actions:
                if isinstance(action, actions.TableCommit):
                    info = action.to_action_dict()["commitInfo"]
                    info["version"] = version
                    yield info

    @property
    def version_to_write(self) -> int:
        """The next log version."""
        try:
            return 1 + self.versions[-1]
        except ValueError:
            return 0

    def schema(self) -> Schema:
        """The latest schema in the log."""
        for v in reversed(self.versions):
            for a in self.entries[v].actions:
                if isinstance(a, actions.TableMetadata):
                    return a.schema
        raise RuntimeError("No schema found in log entries")

    def add_actions(self) -> dict[str, actions.Add]:
        """Add actions as of the latest version."""
        adds = dict()
        for v in self.versions:
            entry = self.entries[v]
            for add in entry.add_actions():
                adds[add.path] = add
            for remove in entry.remove_actions():
                with suppress(KeyError):
                    del adds[remove.path]
        return adds

    def partition_columns(self) -> list:
        """The partition columns of the latest version."""
        cols: list | None = list()
        for v in sorted(self.entries.keys(), reverse=True):
            cols = self.entries[v].partition_columns()
            if cols is not None:
                return cols
        return list()

    def validate_partition_by(self, new_partition_by: Iterable[str] | None) -> list:
        """Check if the new partition columns are the same as the existing ones.

        Args:
            new_partition_by (Iterable[str], optional): The new partition columns

        Returns:
            list: The existing partition
        """
        existing_partition_columns = self.partition_columns()
        if new_partition_by is None:
            pass
        elif set(existing_partition_columns) != set(new_partition_by):
            raise ValueError(f"Expected partition columns {existing_partition_columns}, got {new_partition_by}")
        return existing_partition_columns

    def evaluate_schema(self, pyarrow_schema: pa.Schema, write_mode: WriteMode, schema_mode: str) -> Schema:
        """Evaluate a new table schema given write_mode and schema_mode.

        Args:
            pyarrow_schema (pa.Schema): The schema to evaluate
            write_mode (WriteMode): The write mode
            schema_mode (str): The schema mode

        Returns:
            Schema: The evaluated schema
        """
        schema = Schema.from_pyarrow_schema(pyarrow_schema)
        if not self.entries:
            return schema
        else:
            existing_schema = self.schema()
            if WriteMode.append == write_mode:
                if "merge" == schema_mode:
                    schema = existing_schema.merge(schema)
                elif existing_schema != schema:
                    raise ValueError("Schema mismatch")
            return schema

    def entry_for_write_mode(
        self,
        mode,
        schema: Schema,
        add_actions: list[actions.Add],
        partition_by: list | None = None,
    ):
        """Write a new delta log entry.

        Args:
            mode (WriteMode): Write mode.
            schema (Schema): Schema.
            add_actions (list[Add]): Add actions.
            partition_by (list[str], optional): Partition
        """
        partition_by = partition_by or list()
        entry = DeltaLogEntry()
        if 0 == self.version_to_write:
            entry = DeltaLogEntry.create_table(self.loc.path, schema, partition_by, add_actions)
        elif WriteMode.append == mode:
            entry = DeltaLogEntry.append_table(partition_by, add_actions, schema)
        elif WriteMode.overwrite == mode:
            existing_add_actions = self.add_actions().values()
            entry = DeltaLogEntry.overwrite_table(partition_by, existing_add_actions, add_actions)
        return entry

    def commit(self, entry: DeltaLogEntry, context = nullcontext) -> "DeltaLog":
        if 0 == self.version_to_write:
            self.loc.mkdir(exists_ok=True)
        loc = self.loc.append_path(utils.filename_for_version(self.version_to_write))
        with context(loc):
            with loc.open(mode="w") as fh:
                entry.write(fh)
            return type(self).with_location(self.loc)
