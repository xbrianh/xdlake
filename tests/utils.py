import random

import numpy as np
import pyarrow as pa
from pandas.testing import assert_frame_equal


class TableGen:
    def __init__(self, columns=["bob", "sue", "george", "rebecca", "morgain"]):
        self.columns = columns
        self.categoricals = {
            "cats": ["S", "A", "D"],
            "bats": ["F", "G", "H"],
        }
        self.order_parm = 0

    def __next__(self) -> pa.Table:
        t = pa.table(
            [np.random.random(11) for _ in range(len(self.columns))],
            names = self.columns,
        )

        order = [float(i) + self.order_parm for i in range(len(t))]
        self.order_parm += len(t)

        for name, choices in self.categoricals.items():
            t = t.append_column(name, [random.choice(choices) for _ in range(len(t))])
        t = t.append_column("order", pa.array(order, pa.float64()))
        return t

def assert_arrow_table_equal(a, b):
    a = a.to_pandas().sort_values("order").sort_index(axis=1).reset_index(drop=True)
    b = b.to_pandas().sort_values("order").sort_index(axis=1).reset_index(drop=True)
    assert_frame_equal(a, b)


