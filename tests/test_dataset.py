import os
import unittest
from uuid import uuid4

import pyarrow as pa

from xdlake import dataset

from tests.utils import TableGenMixin, assert_arrow_table_equal


class TestDataset(TableGenMixin, unittest.TestCase):
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
        table = self.gen_table(additional_cols="foo")
        ds = pa.dataset.dataset(self.gen_table(additional_cols="bar"))
        loc = os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
        pa.dataset.write_dataset(self.gen_table(additional_cols="baz"), loc, format="parquet")
        paths = [os.path.join(root, f)
                 for root, _, files in os.walk(loc)
                 for f in files]
        dsr = dataset.resolve([table, ds, *record_batches, *paths], schema_mode="merge")
        self.assertIsInstance(dsr, pa.dataset.UnionDataset)
        assert_arrow_table_equal(pa.concat_tables(self.tables, promote_options="default"), dsr.to_table())

    def test_dataset_schemas_common(self):
        table = self.gen_table()
        original_schema = table.schema
        record_batches = self.gen_table(additional_cols="foo").to_batches()
        ds = pa.dataset.dataset(self.gen_table(additional_cols="bar"))
        loc = os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
        pa.dataset.write_dataset(self.gen_table(additional_cols="baz"), loc, format="parquet")
        paths = [os.path.join(root, f)
                 for root, _, files in os.walk(loc)
                 for f in files]
        dsr = dataset.resolve([table, ds, *record_batches, *paths])
        self.assertIsInstance(dsr, pa.dataset.UnionDataset)
        self.assertEqual(dsr.schema, original_schema)


if __name__ == '__main__':
    unittest.main()
