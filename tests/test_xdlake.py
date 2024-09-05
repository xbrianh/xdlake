import os
import unittest
import warnings
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset
import pandas as pd

import xdlake

from tests.utils import TableGenMixin, assert_pandas_dataframe_equal, assert_arrow_table_equal, AzureSucksCredential


class TestXdLake(TableGenMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        warnings.simplefilter("ignore", DeprecationWarning)
        self.partition_by = list(self.table_gen.categoricals.keys())

    def tearDown(self):
        super().tearDown()
        xdlake.storage._filesystems = dict()

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
            versions.append(xdl.version)
            self.assertNotIn(None, versions)
            assert_arrow_table_equal(t, xdl.to_pyarrow_table())
            self._test_clone(xdl)

        with self.subTest("create as version"):
            at = xdlake.DeltaTable(xdl.loc, version=versions[-2]).to_pyarrow_table()
            assert_arrow_table_equal(df_expected, at)

        with self.subTest("load as version"):
            at = xdl.load_as_version(versions[-2]).to_pyarrow_table()
            assert_arrow_table_equal(df_expected, at)

        self._test_delete(xdl)

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

        self._test_clone(xdl)

    def test_remote_log(self):
        arrow_tables = [self.gen_table() for _ in range(3)]
        expected = pa.concat_tables(arrow_tables)
        tests = [
            (f"{self.scratch_folder}/{uuid4()}", f"{self.scratch_folder}/{uuid4()}"),
            (f"s3://test-xdlake/tests/{uuid4()}", f"{self.scratch_folder}/{uuid4()}"),
            (f"{self.scratch_folder}/{uuid4()}", f"s3://test-xdlake/tests/{uuid4()}"),
        ]
        for data_loc, log_loc in tests:
            xdl = xdlake.DeltaTable(data_loc, log_loc)
            with self.subTest(data_loc=data_loc, log_loc=log_loc):
                for at in arrow_tables:
                    xdl = xdl.write(at)
                assert_arrow_table_equal(expected, xdl.to_pyarrow_table())
                self._test_clone(xdl)
                self._test_delete(xdl)

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

    def test_s3(self):
        partition_by = self.partition_by[:1]
        arrow_tables = [self.gen_table() for _ in range(3)]
        xdl = xdlake.DeltaTable(f"s3://test-xdlake/tests/{uuid4()}")
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=partition_by)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        self._test_clone(xdl)
        self._test_delete(xdl)

    def test_gs(self):
        partition_by = self.partition_by[:1]
        arrow_tables = [self.gen_table() for _ in range(3)]
        xdl = xdlake.DeltaTable(f"gs://test-xdlake/tests/{uuid4()}")
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=partition_by)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        self._test_clone(xdl)
        self._test_delete(xdl)

    def test_azure_storage(self):
        partition_by = self.partition_by[:1]
        arrow_tables = [self.gen_table() for _ in range(3)]

        storage_options = {
            "account_name": "xdlake",
            "credential": AzureSucksCredential(),
        }
        xdl = xdlake.DeltaTable(f"az://test-xdlake/tests/{uuid4()}", storage_options=storage_options)
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=partition_by)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        xdlake.storage.register_default_filesystem_for_protocol("az", storage_options=storage_options)
        self._test_clone(xdl)
        self._test_delete(xdl)

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

        self._test_delete(xdl)

    def test_import_refs(self):
        paths = [os.path.join(f"{self.scratch_folder}", f"{uuid4()}", f"{uuid4()}.parquet") for _ in range(2)]
        paths += [f"s3://test-xdlake/{uuid4()}.parquet" for _ in range(2)]
        paths += [f"gs://test-xdlake/{uuid4()}.parquet" for _ in range(2)]
        paths += [f"az://test-xdlake/{uuid4()}.parquet" for _ in range(2)]
        storage_options = {
            "account_name": "xdlake",
            "credential": AzureSucksCredential(),
        }
        xdlake.storage.register_default_filesystem_for_protocol("az", storage_options=storage_options)
        arrow_tables, written_files = self.gen_parquets(locations=paths)
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}").import_refs(written_files)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        self._test_clone(xdl)
        self._test_delete(xdl)

    def test_import_refs_with_partitions(self):
        storage_options = {
            "account_name": "xdlake",
            "credential": AzureSucksCredential(),
        }
        xdlake.storage.register_default_filesystem_for_protocol("az", storage_options=storage_options)
        hive_partition_schema = pa.unify_schemas([self.table_gen.categorical_schema, pa.schema([("bool_", pa.bool_())])])
        partitionings = {
            "hive": pyarrow.dataset.partitioning(flavor="hive", schema=hive_partition_schema),
            "filename": pyarrow.dataset.partitioning(flavor="filename", schema=self.table_gen.categorical_schema),
            None: pyarrow.dataset.partitioning(flavor=None, schema=self.table_gen.categorical_schema),
        }

        datasets = list()
        arrow_tables = list()
        for flavor, partitioning in partitionings.items():
            foreign_refs_loc = f"az://test-xdlake/{uuid4()}"  # os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
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
        self._test_clone(xdl)
        self._test_delete(xdl)

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
        self._test_delete(cloned)

    def _test_clone(self, xdl: xdlake.DeltaTable) -> xdlake.DeltaTable:
        self.assertLess(0, len(xdl.versions))
        cloned = xdl.clone(f"{self.scratch_folder}/{uuid4()}")

        def assert_version_equal(v: int):
            assert_arrow_table_equal(
                xdl.load_as_version(v).to_pyarrow_table(),
                cloned.load_as_version(v).to_pyarrow_table(),
            )

        with ThreadPoolExecutor() as e:
            for _ in e.map(assert_version_equal, xdl.versions):
                pass

        return cloned

    def test_delete(self):
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        arrow_tables = [self.gen_table() for _ in range(3)]
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=self.partition_by)
        self._test_delete(xdl)
        self._test_clone(xdl)

    def _test_delete(self, xdl: xdlake.DeltaTable):
        exp = (
            (pc.field("cats") == pc.scalar("A"))
            |
            (pc.field("float64") > pc.scalar(0.9))
        )
        deleted = xdl.delete(exp)
        with self.subTest("Should have actually deleted something"):
            self.assertLess(deleted.to_pyarrow_dataset().count_rows(), xdl.to_pyarrow_dataset().count_rows())
        with self.subTest("Should aggree with expected"):
            assert_arrow_table_equal(xdl.to_pyarrow_table().filter(~exp), deleted.to_pyarrow_table())

    def test_from_pandas(self):
        """A common usage pattern is to derive arrow tables from pandas frames. This surprisingly caused a failure due
        to unhashable metadata in the schema!
        """
        frames = [self.gen_table().to_pandas() for _ in range(3)]
        arrow_tables = [pa.Table.from_pandas(df) for df in frames]
        xdl = xdlake.DeltaTable(f"s3://test-xdlake/tests/{uuid4()}")
        for at in arrow_tables:
            xdl = xdl.write(at)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        self._test_delete(xdl)
        self._test_clone(xdl)


if __name__ == '__main__':
    unittest.main()
