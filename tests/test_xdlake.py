import os
import unittest
import warnings
from contextlib import nullcontext
from uuid import uuid4

import pyarrow as pa
import pyarrow.dataset

import xdlake

from tests.utils import TableGenMixin, assert_arrow_table_equal


class TestXdLake(TableGenMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        warnings.simplefilter("ignore", DeprecationWarning)
        self.partition_by = list(self.table_gen.categoricals.keys())

    def test_append_and_overwrite(self):
        loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(3)]

        versions = [xdlake.Writer.write(loc, arrow_table, partition_by=self.partition_by) for arrow_table in arrow_tables]
        self.assertNotIn(None, versions)
        self.assertEqual(versions, xdlake.DeltaTable(loc).versions())

        with self.subTest(mode="append"):
            df_expected = pa.concat_tables(arrow_tables)
            df = xdlake.DeltaTable(loc).to_pyarrow_table()
            assert_arrow_table_equal(df_expected, df)
            self._test_clone(loc)

        with self.subTest(mode="overwrite"):
            t = self.gen_table()
            new_version = xdlake.Writer.write(loc, t, partition_by=self.partition_by, mode="overwrite")
            versions.append(new_version)
            self.assertNotIn(None, versions)
            df = xdlake.DeltaTable(loc).to_pyarrow_table()
            assert_arrow_table_equal(t, df)
            self._test_clone(loc)

        with self.subTest("create as version"):
            df = xdlake.DeltaTable(loc, version=versions[-2]).to_pyarrow_table()
            assert_arrow_table_equal(df_expected, df)

        with self.subTest("load as version"):
            df = xdlake.DeltaTable(loc).load_as_version(versions[-2]).to_pyarrow_table()
            assert_arrow_table_equal(df_expected, df)


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
                with self.subTest(mode=mode, initial_partitions=initial_partitions, partitions=partitions):
                    loc = f"{self.scratch_folder}/{uuid4()}"
                    xdlake.Writer.write(loc, self.gen_table(), partition_by=initial_partitions)
                    with self.assertRaises(ValueError) if should_raise else nullcontext():
                        xdlake.Writer.write(loc, self.gen_table(), partition_by=partitions, mode=mode)

    def test_schema_change(self):
        loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = {
            "original_schema": self.gen_table(),
            "new_schema": self.gen_table(additional_cols=["new_column"]),
        }

        xdlake.Writer.write(loc, arrow_tables["original_schema"], mode="append")

        with self.subTest("should raise"):
            with self.assertRaises(ValueError):
                xdlake.Writer.write(loc, arrow_tables["new_schema"], mode="append")

        with self.subTest("should work"):
            xdlake.Writer.write(loc, arrow_tables["new_schema"], mode="append", schema_mode="merge")

        assert_arrow_table_equal(
            pa.concat_tables(arrow_tables.values(), promote_options="default"),
            xdlake.DeltaTable(loc).to_pyarrow_table(),
        )

        self._test_clone(loc)

    def test_remote_log(self):
        arrow_tables = [self.gen_table() for _ in range(3)]
        expected = pa.concat_tables(arrow_tables)
        tests = [
            (f"{self.scratch_folder}/{uuid4()}", f"{self.scratch_folder}/{uuid4()}"),
            (f"s3://test-xdlake/tests/{uuid4()}", f"{self.scratch_folder}/{uuid4()}"),
            (f"{self.scratch_folder}/{uuid4()}", f"s3://test-xdlake/tests/{uuid4()}"),
        ]
        for data_loc, log_loc in tests:
            with self.subTest(data_loc=data_loc, log_loc=log_loc):
                for at in arrow_tables:
                    xdlake.Writer.write(data_loc, at, log_loc=log_loc)

                assert_arrow_table_equal(
                    expected,
                    xdlake.DeltaTable(data_loc, log_loc).to_pyarrow_table(),
                )
                self._test_clone(data_loc, src_log_loc=log_loc)

    def test_write_mode_error_ignore(self):
        loc = f"{self.scratch_folder}/{uuid4()}"
        expected = self.gen_table()
        xdlake.Writer.write(loc, expected)

        with self.subTest("should raise FileExistsError"):
            with self.assertRaises(FileExistsError):
                xdlake.Writer.write(loc, self.gen_table(), mode="error")
            df = xdlake.DeltaTable(loc).to_pyarrow_table()
            assert_arrow_table_equal(expected, df)

        with self.subTest("should not write to table, and not raise"):
            xdlake.Writer.write(loc, self.gen_table(), mode="ignore")
            df = xdlake.DeltaTable(loc).to_pyarrow_table()
            assert_arrow_table_equal(expected, df)

    def test_s3(self):
        partition_by = self.partition_by[:1]
        loc = f"s3://test-xdlake/tests/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(3)]

        for at in arrow_tables:
            xdlake.Writer.write(loc, at, partition_by=partition_by)

        assert_arrow_table_equal(
            pa.concat_tables(arrow_tables),
            xdlake.DeltaTable(loc).to_pyarrow_table()
        )

        self._test_clone(loc)

    def test_gs(self):
        partition_by = self.partition_by[:1]
        loc = f"gs://test-xdlake/tests/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(3)]

        for at in arrow_tables:
            xdlake.Writer.write(loc, at, partition_by=partition_by)

        assert_arrow_table_equal(
            pa.concat_tables(arrow_tables),
            xdlake.DeltaTable(loc).to_pyarrow_table()
        )

        self._test_clone(loc)

    def test_write_kind(self):
        partition_by = self.partition_by[:1]

        arrow_tables, paths = self.gen_parquets(
            locations=[os.path.join(f"{self.scratch_folder}", f"{uuid4()}.parquet") for _ in range(3)]
        )
        ds = pa.dataset.dataset(paths)
        expected = pa.concat_tables(arrow_tables)

        with self.subTest("write pyarrow dataset"):
            loc = f"{self.scratch_folder}/{uuid4()}"
            xdlake.Writer.write(loc, ds, partition_by=partition_by)
            assert_arrow_table_equal(expected, xdlake.DeltaTable(loc).to_pyarrow_table())
            self._test_clone(loc)

        with self.subTest("write pyarrow record batches"):
            loc = f"{self.scratch_folder}/{uuid4()}"
            for batch in ds.to_batches():
                xdlake.Writer.write(loc, batch, partition_by=partition_by)
            assert_arrow_table_equal(expected, xdlake.DeltaTable(loc).to_pyarrow_table())
            self._test_clone(loc)

    def test_import_refs(self):
        loc = f"{self.scratch_folder}/{uuid4()}"

        paths = [os.path.join(f"{self.scratch_folder}", f"{uuid4()}", f"{uuid4()}.parquet") for _ in range(3)]
        paths += [f"s3://test-xdlake/{uuid4()}.parquet" for _ in range(3)]
        arrow_tables, written_files = self.gen_parquets(locations=paths)

        xdlake.Writer.import_refs(loc, written_files)

        assert_arrow_table_equal(
            pa.concat_tables(arrow_tables),
            xdlake.DeltaTable(loc).to_pyarrow_table()
        )

        self._test_clone(loc)

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
            foreign_refs_loc = f"gs://test-xdlake/{uuid4()}"  # os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
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

        loc = f"{self.scratch_folder}/{uuid4()}"
        xdlake.Writer.import_refs(loc, datasets, partition_by=self.partition_by)

        assert_arrow_table_equal(
            pa.concat_tables(arrow_tables),
            xdlake.DeltaTable(loc).to_pyarrow_table()
        )

        self._test_clone(loc)

    def test_clone(self):
        partition_by = self.partition_by[:1]
        src_loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(3)]
        more_arrow_tables = [self.gen_table() for _ in range(2)]
        for at in arrow_tables:
            xdlake.Writer.write(src_loc, at, partition_by=partition_by)
        df = xdlake.DeltaTable(src_loc).to_pyarrow_table()
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), df)
        self._test_clone(src_loc)

        dst_loc = self._test_clone(src_loc)

        with self.subTest("Should be possible to write to cloned table"):
            for at in more_arrow_tables:
                xdlake.Writer.write(dst_loc, at, partition_by=partition_by)
            assert_arrow_table_equal(pa.concat_tables([*arrow_tables, *more_arrow_tables]), xdlake.DeltaTable(dst_loc).to_pyarrow_table())

    def _test_clone(self, src_loc, src_log_loc: str | None = None) -> str:
        expected = xdlake.DeltaTable(src_loc, log_loc=src_log_loc).to_pyarrow_table()

        with self.subTest("clone to local"):
            dst_loc = f"{self.scratch_folder}/{uuid4()}"
            xdlake.clone(src_loc, dst_loc, src_log_loc=src_log_loc)
            assert_arrow_table_equal(xdlake.DeltaTable(dst_loc).to_pyarrow_table(), expected)

        tested_something = False
        for version in xdlake.DeltaTable(src_loc, log_loc=src_log_loc).versions():
            tested_something = True
            with self.subTest("clone agrees", version=version):
                assert_arrow_table_equal(
                    xdlake.DeltaTable(src_loc, log_loc=src_log_loc, version=version).to_pyarrow_table(),
                    xdlake.DeltaTable(dst_loc, version=version).to_pyarrow_table(),
                )
        self.assertTrue(tested_something)

        return dst_loc


if __name__ == '__main__':
    unittest.main()
