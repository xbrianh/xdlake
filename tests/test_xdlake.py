import shutil
import random
import unittest

import numpy as np
import pyarrow
import deltalake

import xdlake


cats = ["S", "A", "D"]

class TestXdLake(unittest.TestCase):
    def test_xdlake(self):
        test_dir = "testdl"
        shutil.rmtree(test_dir, ignore_errors=True)

        for _ in range(4):
            t = pyarrow.table(
                [np.random.random(11) for _ in range(5)],
                names = ["bob", "sue", "george", "rebecca", "morgain"],
            )
            t = t.append_column("cats", [random.choice(cats) for _ in range(len(t))])
            xdlake.write(test_dir, t, partition_by=["cats"])

        t = deltalake.DeltaTable("testdl")
        t.to_pandas()

    def test_storage(self):
        for url in ["file://foo/bar/baz", "s3://foo/bar/baz"]:
            s = xdlake.StorageLocation(url)
            print(s.bucket)
            print(s.path)
            print(s.url)
            print()

    def write_deltalake(self):
        test_dir = "tdl"
        shutil.rmtree(test_dir, ignore_errors=True)

        for _ in range(4):
            t = pyarrow.table(
                [np.random.random(11) for _ in range(5)],
                names = ["bob", "sue", "george", "rebecca", "morgain"],
            )
            deltalake.write_deltalake("tdl", t, mode="append")


if __name__ == '__main__':
    unittest.main()
