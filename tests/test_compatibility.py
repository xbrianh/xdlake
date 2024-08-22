import unittest
from uuid import uuid4

import deltalake

import xdlake

from tests.utils import TableGenMixin, assert_arrow_table_equal


class TestCompatibility(TableGenMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.partition_by = list(self.table_gen.categoricals.keys())

    def test_append_and_overwrite(self):
        loc = f"{self.scratch_folder}/{uuid4()}"

        for _ in range(3):
            xdlake.Writer.write(loc, self.gen_table(), partition_by=self.partition_by)

        with self.subTest("should aggree", mode="append"):
            df_expected = deltalake.DeltaTable(loc)
            df = xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
            assert_arrow_table_equal(df_expected, df)

        with self.subTest("should aggree", mode="overwrite"):
            xdlake.Writer.write(loc, self.gen_table(), partition_by=self.partition_by, mode="overwrite")
            df_expected = deltalake.DeltaTable(loc)
            df = xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
            assert_arrow_table_equal(df_expected, df)

    def test_schema_change(self):
        loc = f"{self.scratch_folder}/{uuid4()}"
        dl_loc = f"{self.scratch_folder}/{uuid4()}"

        tables = [self.gen_table() for _ in range(3)]
        table_new_schema = self.gen_table(additional_cols=["new_column"])

        for t in tables:
            xdlake.Writer.write(loc, t, mode="append")
            deltalake.write_deltalake(dl_loc, t, mode="append")

        with self.subTest("should raise"):
            with self.assertRaises(ValueError):
                xdlake.Writer.write(loc, table_new_schema, mode="append")

        with self.subTest("should work"):
            xdlake.Writer.write(loc, table_new_schema, mode="append", schema_mode="merge")
            deltalake.write_deltalake(dl_loc, table_new_schema, mode="append", schema_mode="merge", engine="rust")

        assert_arrow_table_equal(
            deltalake.DeltaTable(dl_loc).to_pyarrow_table(),
            xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table(),
        )


if __name__ == '__main__':
    unittest.main()
