import os
import pandas as pd
import pathlib as pl
import utils.pickle_adapter  # noqa: F401  # activa el adaptador según BACKUP_FMT

os.environ.setdefault("SYMBIOS_TEMP", "./temp_ediciones")
tmp = pl.Path(os.getenv("SYMBIOS_TEMP"))
tmp.mkdir(parents=True, exist_ok=True)

df = pd.DataFrame({"a": [1, 2, 3]})
pkl_target = tmp / "demo_buffer.pkl"  # apuntamos a .pkl, se guardará .parquet

pd.to_pickle(df, pkl_target)
obj = pd.read_pickle(pkl_target)

print(
    "WROTE_PARQUET_EXISTS:",
    (tmp / "demo_buffer.parquet").exists(),
    "READ_SHAPE:",
    getattr(obj, "shape", None),
)
