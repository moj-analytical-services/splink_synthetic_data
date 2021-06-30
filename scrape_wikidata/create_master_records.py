# %%
import glob
import pandas as pd

# %%

files_h = sorted(glob.glob("processed_data/step_1_one_line_per_person/*.parquet"))
files_pc = sorted(glob.glob("processed_data/step_2_person_postcode_lookups/*.parquet"))

# %%
fh = files_h[0]
fpc = files_pc[0]
# %%
df_h = pd.read_parquet(fh)
df_pc = pd.read_parquet(fpc)
# %%
