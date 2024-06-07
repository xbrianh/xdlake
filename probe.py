import random
import deltalake
import pandas as pd
import numpy as np


cats = ["S", "A", "D"]

df = pd.DataFrame(np.random.random((11, 5)))
df.columns = ["bob", "sue", "george", "rebecca", "morgain"]
df["cats"] = [random.choice(cats) for _ in range(len(df))]
deltalake.write_deltalake("tdl", df, partition_by=["cats"])

df = pd.DataFrame(np.random.random((11, 6)))
df.columns = ["bob", "sue", "george", "rebecca", "morgain", "jane"]
df["cats"] = [random.choice(cats) for _ in range(len(df))]
deltalake.write_deltalake("tdl", df, mode="append", schema_mode="merge", engine="rust", partition_by=["cats"])

t = deltalake.DeltaTable("tdl")
print(t.to_pandas())
