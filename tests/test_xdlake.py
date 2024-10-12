import os
import unittest
from contextlib import contextmanager, nullcontext
from uuid import uuid4
from unittest import mock

import pyarrow as pa
import pyarrow.dataset
import pandas as pd

import xdlake

from tests.base_xdlake_test import BaseXdlakeTest
from tests.utils import assert_pandas_dataframe_equal, assert_arrow_table_equal


class TestXdLake(BaseXdlakeTest):
    def test_to_pandas(self):
        arrow_tables = [self.gen_table() for _ in range(3)]
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=self.partition_by)
        assert_pandas_dataframe_equal(
            pd.concat([at.to_pandas() for at in arrow_tables]),
            xdl.to_pandas(),
        )

    def test_append_and_overwrite(self):
        arrow_tables = [self.gen_table() for _ in range(3)]
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=self.partition_by)
        versions = list(xdl.versions)

        with self.subTest(mode="append"):
            df_expected = pa.concat_tables(arrow_tables)
            df = xdl.to_pyarrow_table()
            assert_arrow_table_equal(df_expected, df)
            self._test_clone(xdl)

        with self.subTest(mode="overwrite"):
            t = self.gen_table()
            xdl = xdl.write(t, partition_by=self.partition_by, mode="overwrite")
            versions.append(xdl.version())
            self.assertNotIn(None, versions)
            assert_arrow_table_equal(t, xdl.to_pyarrow_table())
            self._test_clone(xdl)

        with self.subTest("create as version"):
            at = xdlake.DeltaTable(xdl.loc, version=versions[-2]).to_pyarrow_table()
            assert_arrow_table_equal(df_expected, at)

        with self.subTest("load as version"):
            at = xdl.load_as_version(versions[-2]).to_pyarrow_table()
            assert_arrow_table_equal(df_expected, at)

        xdl = self._test_delete(xdl)
        xdl = self._test_restore(xdl)
        xdl = self._test_clone(xdl)

    def test_restore(self):
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        for _ in range(5):
            xdl = xdl.write(self.gen_table(), partition_by=self.partition_by)
        self._test_restore(xdl)
        return
        xdl = xdl.restore(2)
        self.assertEqual(5, xdl.version())
        assert_arrow_table_equal(
            xdl.to_pyarrow_table(),
            xdl.load_as_version(2).to_pyarrow_table(),
        )

    def test_partition_column_change(self):
        tests = [
            ([], ["bats"], True),
            (["cats"], [], True),
            (["cats"], ["bats"], True),
            (["cats", "bats"], [], True),
            ([], ["cats", "bats"], True),
            (["cats", "bats"], ["cats", "bats"], False),
            (["cats", "bats"], ["bats", "cats"], False),
            (["cats", "bats"], None, False),
        ]

        for mode in ["append", "overwrite"]:
            for initial_partitions, partitions, should_raise in tests:
                with self.subTest(mode=mode, initial_partitions=initial_partitions, new_partitions=partitions):
                    xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}").write(self.gen_table(), partition_by=initial_partitions)
                    with self.assertRaises(ValueError) if should_raise else nullcontext():
                        xdl.write(self.gen_table(), partition_by=partitions, mode=mode)

    def test_schema_change(self):
        arrow_tables = {
            "original_schema": self.gen_table(),
            "new_schema": self.gen_table(additional_cols=["new_column"]),
        }

        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}").write(arrow_tables["original_schema"], mode="append")

        with self.subTest("should raise"):
            with self.assertRaises(ValueError):
                xdl.write(arrow_tables["new_schema"], mode="append")

        with self.subTest("should work"):
            xdl = xdl.write(arrow_tables["new_schema"], mode="append", schema_mode="merge")

        assert_arrow_table_equal(
            pa.concat_tables(arrow_tables.values(), promote_options="default"),
            xdl.to_pyarrow_table(),
        )

        xdl = self._test_clone(xdl)
        xdl = self._test_delete(xdl)
        xdl = self._test_restore(xdl, 0)

    def test_remote_log(self):
        arrow_tables = [self.gen_table() for _ in range(3)]
        expected = pa.concat_tables(arrow_tables)
        tests = [
            (f"{self.scratch_folder}/{uuid4()}", f"{self.scratch_folder}/{uuid4()}"),
        ]
        for data_loc, log_loc in tests:
            xdl = xdlake.DeltaTable(data_loc, log_loc)
            with self.subTest(data_loc=data_loc, log_loc=log_loc):
                for at in arrow_tables:
                    xdl = xdl.write(at)
                assert_arrow_table_equal(expected, xdl.to_pyarrow_table())
                xdl = self._test_clone(xdl)
                xdl = self._test_delete(xdl)
                xdl = self._test_restore(xdl)

    def test_write_mode_error_ignore(self):
        expected = self.gen_table()
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}").write(expected)

        with self.subTest("should raise FileExistsError"):
            with self.assertRaises(FileExistsError):
                xdl.write(self.gen_table(), mode="error")
            assert_arrow_table_equal(expected, xdl.to_pyarrow_table())

        with self.subTest("should not write to table, and not raise"):
            xdl = xdl.write(self.gen_table(), mode="ignore")
            assert_arrow_table_equal(expected, xdl.to_pyarrow_table())

    def test_write_kind(self):
        partition_by = self.partition_by[:1]

        arrow_tables, paths = self.gen_parquets(
            locations=[os.path.join(f"{self.scratch_folder}", f"{uuid4()}.parquet") for _ in range(3)]
        )
        ds = pa.dataset.dataset(paths)
        expected = pa.concat_tables(arrow_tables)

        with self.subTest("write pyarrow dataset"):
            xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}").write(ds, partition_by=partition_by)
            assert_arrow_table_equal(expected, xdl.to_pyarrow_table())
            self._test_clone(xdl)

        with self.subTest("write pyarrow record batches"):
            xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
            for batch in ds.to_batches():
                xdl = xdl.write(batch, partition_by=partition_by)
            assert_arrow_table_equal(expected, xdl.to_pyarrow_table())
            self._test_clone(xdl)

        xdl = self._test_delete(xdl)
        xdl = self._test_restore(xdl)
        xdl = self._test_clone(xdl)

    def test_import_refs(self):
        paths = [os.path.join(f"{self.scratch_folder}", f"{uuid4()}", f"{uuid4()}.parquet") for _ in range(2)]
        arrow_tables, written_files = self.gen_parquets(locations=paths)
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}").import_refs(written_files)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        self._test_clone(xdl)
        self._test_delete(xdl)

    def test_import_refs_with_partitions(self):
        hive_partition_schema = pa.unify_schemas([self.table_gen.categorical_schema, pa.schema([("bool_", pa.bool_())])])
        partitionings = {
            "hive": pyarrow.dataset.partitioning(flavor="hive", schema=hive_partition_schema),
            "filename": pyarrow.dataset.partitioning(flavor="filename", schema=self.table_gen.categorical_schema),
            None: pyarrow.dataset.partitioning(flavor=None, schema=self.table_gen.categorical_schema),
        }

        datasets = list()
        arrow_tables = list()
        for flavor, partitioning in partitionings.items():
            foreign_refs_loc = os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
            new_tables, written_files = self.gen_parquets(
                locations=[foreign_refs_loc],
                partitioning=partitioning,
            )
            arrow_tables.extend(new_tables)
            ds = pyarrow.dataset.dataset(
                written_files,
                partitioning=partitioning,
                partition_base_dir=foreign_refs_loc,
                filesystem=xdlake.storage.get_filesystem(foreign_refs_loc),
            )
            datasets.append(ds)

        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}").import_refs(datasets, partition_by=self.partition_by)

        assert_arrow_table_equal(
            pa.concat_tables(arrow_tables),
            xdl.to_pyarrow_table()
        )
        xdl = self._test_clone(xdl)
        xdl = self._test_delete(xdl)
        xdl = self._test_restore(xdl)

    def test_clone(self):
        partition_by = self.partition_by[:1]
        arrow_tables = [self.gen_table() for _ in range(3)]
        more_arrow_tables = [self.gen_table() for _ in range(2)]
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=partition_by)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())

        cloned = self._test_clone(xdl)

        with self.subTest("Should be possible to write to cloned table"):
            for at in more_arrow_tables:
                cloned = cloned.write(at, partition_by=partition_by)
            assert_arrow_table_equal(pa.concat_tables([*arrow_tables, *more_arrow_tables]), cloned.to_pyarrow_table())
        self._test_delete(xdl)
        xdl = self._test_delete(cloned)
        self._test_restore(xdl)

    def test_delete(self):
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        arrow_tables = [self.gen_table() for _ in range(3)]
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=self.partition_by)
        xdl = self._test_delete(xdl)
        xdl = self._test_clone(xdl)
        xdl = self._test_restore(xdl)

    def test_from_pandas(self):
        """A common usage pattern is to derive arrow tables from pandas frames. This surprisingly caused a failure due
        to unhashable metadata in the schema!
        """
        frames = [self.gen_table().to_pandas() for _ in range(3)]
        arrow_tables = [pa.Table.from_pandas(df) for df in frames]
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        for at in arrow_tables:
            xdl = xdl.write(at)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        self._test_delete(xdl)
        self._test_clone(xdl)

    def test_commit(self):
        loc = f"{self.scratch_folder}"
        expected_path = f"{loc}/_delta_log/00000000000000000000.json"

        xdl = xdlake.DeltaTable(loc)
        os.mkdir(os.path.dirname(expected_path))
        with open(expected_path, "w") as fh:
            fh.write("")
        with self.assertRaises(FileExistsError):
            xdl = xdl.commit(mock.MagicMock())

    def test_commit_context(self):
        loc = f"{self.scratch_folder}"
        expected_path = f"{loc}/_delta_log/00000000000000000000.json"
        mock_obj = mock.MagicMock()

        class TestDeltaTable(xdlake.DeltaTable):
            @contextmanager
            def commit_context(s, loc):
                try:
                    mock_obj(loc.path)
                    yield
                finally:
                    pass

        xdl = TestDeltaTable(loc)
        mock_obj.assert_not_called()
        xdl.commit(mock.MagicMock())
        mock_obj.assert_called_once_with(expected_path)

    def test_history(self):
        arrow_tables = [self.gen_table() for _ in range(3)]
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        for at in arrow_tables:
            xdl = xdl.write(at)
        self.assertEqual(
            [2, 1, 0],
            [info["version"] for info in xdl.history()],
        )
        self.assertEqual(
            [0, 1, 2],
            [info["version"] for info in xdl.history(reverse=False)],
        )

    def test_write_pandas(self):
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        arrow_tables = [self.gen_table() for _ in range(3)]
        xdl = xdl.write([at.to_pandas() for at in arrow_tables])
        assert_arrow_table_equal(
            pa.concat_tables(arrow_tables),
            xdl.to_pyarrow_table()
        )

    def test_write_with_generator(self):
        arrow_tables = [self.gen_table() for _ in range(3)]

        def gen_tables():
            for at in arrow_tables:
                yield at

        def gen_frames():
            for at in arrow_tables:
                yield at.to_pandas()

        with self.subTest("generate arrow tables"):
            xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
            xdl = xdl.write(gen_tables())
            assert_arrow_table_equal(
                pa.concat_tables(arrow_tables),
                xdl.to_pyarrow_table()
            )

        with self.subTest("generate pandas dataframes"):
            xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
            xdl = xdl.write(gen_frames())
            assert_arrow_table_equal(
                pa.concat_tables(arrow_tables),
                xdl.to_pyarrow_table()
            )

    def test_file_uris(self):
        number_of_writes = 3
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        for _ in range(number_of_writes):
            xdl = xdl.write(self.gen_table())
        self.assertEqual(number_of_writes, len(xdl.file_uris()))
        for uri in xdl.file_uris():
            xdl.loc.fs.head(uri, size=1)

if __name__ == '__main__':
    unittest.main()
