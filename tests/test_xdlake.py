import os
import shutil
import random
import unittest

import numpy as np
import pandas as pd
import pyarrow

import xdlake


cats = ["S", "A", "D"]

class TestXdlake(unittest.TestCase):
    def test_foo(self):
        test_dir = "testdl"
        shutil.rmtree(test_dir, ignore_errors=True)
        t = pyarrow.table(
            [np.random.random(11) for _ in range(5)],
            names = ["bob", "sue", "george", "rebecca", "morgain"],
        )
        t = t.append_column("cats", [random.choice(cats) for _ in range(len(t))])
        xdlake.write(test_dir, t, partition_by=["cats"])

    def probe(self):
        import deltalake
        t = deltalake.DeltaTable("tdl")
        df = t.to_pandas()
        print(df)


if __name__ == '__main__':
    unittest.main()
