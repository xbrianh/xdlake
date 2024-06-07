import shutil
import random
import unittest

import numpy as np
import pyarrow
import deltalake

import xdlake


cats = ["S", "A", "D"]

class TestXdLake(unittest.TestCase):
    def test_foo(self):
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

    def probe(self):
        pass


if __name__ == '__main__':
    unittest.main()
