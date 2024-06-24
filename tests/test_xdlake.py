import os
import unittest
from uuid import uuid4

import pyarrow as pa

import xdlake

from tests.utils import TableGenMixin, assert_arrow_table_equal


class TestXdLake(TableGenMixin, unittest.TestCase):
    def test_append_and_overwrite(self):
        loc = f"{self.scratch_folder}/{uuid4()}"

        for _ in range(3):
            xdlake.Writer.write(loc, self.gen_table(), partition_by=["cats", "bats"])

        with self.subTest(mode="append"):
            df_expected = pa.concat_tables(self.tables)
            df = xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
            assert_arrow_table_equal(df_expected, df)

        with self.subTest(mode="overwrite"):
            t = self.gen_table()
            xdlake.Writer.write(loc, t, partition_by=["cats", "bats"], mode="overwrite")
            df = xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
            assert_arrow_table_equal(t, df)

    def test_schema_change(self):
        loc = f"{self.scratch_folder}/{uuid4()}"

        xdlake.Writer.write(loc, self.gen_table(), mode="append")
        table_new_schema = self.gen_table(additional_cols=["new_column"])

        with self.subTest("should raise"):
            with self.assertRaises(ValueError):
                xdlake.Writer.write(loc, table_new_schema, mode="append")

        with self.subTest("should work"):
            xdlake.Writer.write(loc, table_new_schema, mode="append", schema_mode="merge")

        assert_arrow_table_equal(
            pa.concat_tables(self.tables, promote_options="default"),
            xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table(),
        )

    def test_remote_log(self):
        tables = [self.gen_table() for _ in range(3)]
        expected = pa.concat_tables(tables)
        tests = [
            (f"s3://test-xdlake/tests/{uuid4()}", f"{self.scratch_folder}/{uuid4()}"),
            (f"{self.scratch_folder}/{uuid4()}", f"s3://test-xdlake/tests/{uuid4()}"),
        ]
        for data_loc, log_loc in tests:
            with self.subTest(data_loc=data_loc, log_loc=log_loc):
                for t in tables:
                    xdlake.Writer.write(data_loc, t, log_loc=log_loc)

                assert_arrow_table_equal(
                    expected,
                    xdlake.DeltaTable(data_loc, log_loc).to_pyarrow_dataset().to_table(),
                )

    def test_write_mode_error_ignore(self):
        loc = f"{self.scratch_folder}/{uuid4()}"
        expected = self.gen_table()
        xdlake.Writer.write(loc, expected)

        with self.subTest("should raise FileExistsError"):
            with self.assertRaises(FileExistsError):
                xdlake.Writer.write(loc, self.gen_table(), mode="error")
            df = xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
            assert_arrow_table_equal(expected, df)

        with self.subTest("should not write to table, and not raise"):
            xdlake.Writer.write(loc, self.gen_table(), mode="ignore")
            df = xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
            assert_arrow_table_equal(expected, df)

    def test_s3(self):
        loc = f"s3://test-xdlake/tests/{uuid4()}"
        tables = [self.gen_table() for _ in range(3)]

        for t in tables:
            xdlake.Writer.write(loc, t, partition_by=["cats"])

        assert_arrow_table_equal(
            pa.concat_tables(tables),
            xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
        )

    def test_write_kind(self):
        tables, paths = self.gen_parquets_for_tables(
            table_locs=[os.path.join(f"{self.scratch_folder}", f"{uuid4()}.parquet") for _ in range(3)]
        )
        ds = pa.dataset.dataset(paths)
        expected = pa.concat_tables(tables)

        with self.subTest("write pyarrow dataset"):
            loc = f"{self.scratch_folder}/{uuid4()}"
            xdlake.Writer.write(loc, ds, partition_by=["cats"])
            assert_arrow_table_equal(expected, xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table())

        with self.subTest("write pyarrow record batches"):
            loc = f"{self.scratch_folder}/{uuid4()}"
            for batch in ds.to_batches():
                xdlake.Writer.write(loc, batch, partition_by=["cats"])
            assert_arrow_table_equal(expected, xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table())


if __name__ == '__main__':
    unittest.main()
