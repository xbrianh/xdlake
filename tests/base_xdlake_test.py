import unittest
import warnings
import random
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor

import pyarrow.compute as pc

from tests.utils import TableGenMixin, assert_arrow_table_equal

import xdlake


class BaseXdlakeTest(TableGenMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        warnings.simplefilter("ignore", DeprecationWarning)
        self.partition_by = list(self.table_gen.categoricals.keys())

    def tearDown(self):
        super().tearDown()
        xdlake.storage._filesystems = dict()

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
        return deleted

    def _test_restore(self, xdl: xdlake.DeltaTable, restore_version: int | None = None):
        curr_version = xdl.version()
        restore_version = restore_version or random.choice(xdl.versions)
        restored = xdl.restore(restore_version)
        self.assertEqual(restored.version(), 1 + curr_version)
        assert_arrow_table_equal(
            restored.to_pyarrow_table(),
            restored.load_as_version(restore_version).to_pyarrow_table(),
        )
        return restored

