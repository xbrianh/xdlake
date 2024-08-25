import unittest
from uuid import uuid4

import deltalake
import pyarrow.compute as pc

import xdlake

from tests.utils import TableGenMixin, assert_arrow_table_equal


class TestCompatibility(TableGenMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.partition_by = list(self.table_gen.categoricals.keys())

    def test_append_and_overwrite(self):
        xd_loc = f"{self.scratch_folder}/{uuid4()}"
        dt_loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(3)]
        overwrite_arrow_table = self.gen_table()

        for at in arrow_tables:
            xdlake.Writer.write(xd_loc, at, partition_by=self.partition_by)
            deltalake.write_deltalake(dt_loc, at, partition_by=self.partition_by, mode="append")

        with self.subTest("should aggree", mode="append"):
            assert_arrow_table_equal(deltalake.DeltaTable(xd_loc), xdlake.DeltaTable(dt_loc).to_pyarrow_table())

        with self.subTest("should aggree", mode="overwrite"):
            xdlake.Writer.write(xd_loc, overwrite_arrow_table, partition_by=self.partition_by, mode="overwrite")
            deltalake.write_deltalake(dt_loc, overwrite_arrow_table, partition_by=self.partition_by, mode="overwrite")
            assert_arrow_table_equal(deltalake.DeltaTable(xd_loc), xdlake.DeltaTable(dt_loc).to_pyarrow_table())

    def test_schema_change(self):
        xd_loc = f"{self.scratch_folder}/{uuid4()}"
        dt_loc = f"{self.scratch_folder}/{uuid4()}"

        tables = [self.gen_table() for _ in range(3)]
        table_new_schema = self.gen_table(additional_cols=["new_column"])

        for t in tables:
            xdlake.Writer.write(xd_loc, t, mode="append")
            deltalake.write_deltalake(dt_loc, t, mode="append")

        with self.subTest("xdlake should raise"):
            with self.assertRaises(ValueError):
                xdlake.Writer.write(xd_loc, table_new_schema, mode="append")

        with self.subTest("should work"):
            xdlake.Writer.write(xd_loc, table_new_schema, mode="append", schema_mode="merge")
            deltalake.write_deltalake(dt_loc, table_new_schema, mode="append", schema_mode="merge", engine="rust")

        assert_arrow_table_equal(
            deltalake.DeltaTable(xd_loc).to_pyarrow_table(),
            xdlake.DeltaTable(dt_loc).to_pyarrow_dataset().to_table(),
        )

    def test_delete(self):
        xd_loc = f"{self.scratch_folder}/{uuid4()}"
        dt_loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(23)]

        for at in arrow_tables:
            xdlake.Writer.write(xd_loc, at, partition_by=self.partition_by)
            deltalake.write_deltalake(dt_loc, at, partition_by=self.partition_by, mode="append")

        num_start_rows = xdlake.DeltaTable(xd_loc).to_pyarrow_table().to_pandas().shape[0]
        xdlake.Writer.delete(xd_loc, (((pc.field("float64") > pc.scalar(0.9)) | (pc.field("cats") == pc.scalar("A")))))
        deltalake.DeltaTable(dt_loc).delete("float64 > 0.9 or cats == 'A'")
        num_end_rows = xdlake.DeltaTable(xd_loc).to_pyarrow_table().to_pandas().shape[0]
        with self.subTest("should have actually deleted rows"):
            self.assertLess(num_end_rows, num_start_rows)

        with self.subTest("should aggree"):
            assert_arrow_table_equal(deltalake.DeltaTable(xd_loc), xdlake.DeltaTable(dt_loc).to_pyarrow_table())


if __name__ == '__main__':
    unittest.main()
