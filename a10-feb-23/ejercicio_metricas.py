import pandas as pd
import numpy as np
import requests

API_URL = "http://localhost:8000"

resp = requests.get(f"{API_URL}/export-csv")
resp.raise_for_status()

with open("images_export.csv", "wb") as f:
    f.write(resp.content)

df = pd.read_csv("images_export.csv")

gb = df.groupby("bucket")

mean_width = gb["width"].mean()

mean_height = gb["height"].mean()

std_size = gb["size_bytes"].std()

count_wide = gb.apply(lambda x: np.sum(x["width"].values > 1000))

result = pd.DataFrame({
    "mean_width": mean_width,
    "mean_height": mean_height,
    "std_size_bytes": std_size,
    "count_width_gt_1000": count_wide,
})
print(result)
