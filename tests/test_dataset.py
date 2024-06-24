import os
import unittest
from uuid import uuid4
from tempfile import TemporaryDirectory

import pyarrow as pa

from xdlake import dataset

from tests.utils import TableGen, assert_arrow_table_equal


class TestDataset(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.scratch_folder = self.td.name
        self.table_gen = TableGen()
        self.tables = list()

    def tearDown(self):
        self.td.cleanup()

    def gen_table(self, new_columns: list | str | None = None):
        if new_columns is not None:
            if isinstance(new_columns, str):
                new_columns = [new_columns]
            for col in new_columns:
                if col not in self.table_gen.columns:
                    self.table_gen.columns.append(col)
        t = next(self.table_gen)
        self.tables.append(t)
        return t

    def test_dataset(self):
        table = self.gen_table()
        record_batches = self.gen_table().to_batches()
        ds = pa.dataset.dataset(self.gen_table())
        loc = os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
        pa.dataset.write_dataset(self.gen_table(), loc, format="parquet")
        paths = [os.path.join(root, f)
                 for root, _, files in os.walk(loc)
                 for f in files]
        dsr = dataset.resolve([table, ds, *record_batches, *paths])
        self.assertIsInstance(dsr, pa.dataset.UnionDataset)
        assert_arrow_table_equal(pa.concat_tables(self.tables), dsr.to_table())

    def test_dataset_schemas_merge(self):
        record_batches = self.gen_table().to_batches()
        table = self.gen_table("foo")
        ds = pa.dataset.dataset(self.gen_table("bar"))
        loc = os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
        pa.dataset.write_dataset(self.gen_table("baz"), loc, format="parquet")
        paths = [os.path.join(root, f)
                 for root, _, files in os.walk(loc)
                 for f in files]
        dsr = dataset.resolve([table, ds, *record_batches, *paths], schema_mode="merge")
        self.assertIsInstance(dsr, pa.dataset.UnionDataset)
        assert_arrow_table_equal(pa.concat_tables(self.tables, promote_options="default"), dsr.to_table())

    def test_dataset_schemas_common(self):
        table = self.gen_table()
        original_schema = table.schema
        record_batches = self.gen_table("foo").to_batches()
        ds = pa.dataset.dataset(self.gen_table("bar"))
        loc = os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
        pa.dataset.write_dataset(self.gen_table("baz"), loc, format="parquet")
        paths = [os.path.join(root, f)
                 for root, _, files in os.walk(loc)
                 for f in files]
        dsr = dataset.resolve([table, ds, *record_batches, *paths])
        self.assertIsInstance(dsr, pa.dataset.UnionDataset)
        self.assertEqual(dsr.schema, original_schema)


if __name__ == '__main__':
    unittest.main()
