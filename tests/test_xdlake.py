import os
import shutil
import random
import unittest

import xdlake
import numpy as np
import pandas as pd
import pyarrow


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


if __name__ == '__main__':
    unittest.main()
