# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# Crop area harvested, yield and production by country, crop and year, from FAOSTAT
# (domain QCL, "Crops and livestock products"), as published. The public bulk file needs
# no account; it is read in chunks and only the three crop elements are kept, for crops
# (items with an area harvested), without FAO's group totals (CPC codes starting with F).
# FAO_BULK_FILE may name a copy of the zip dropped on disk (a server without internet
# access); when that file is absent the zip is downloaded. Countries are matched to the
# country table by name: an FAO area with another spelling is left out and listed.
import io
import os
import zipfile

import pandas as pd

BULK_URL = ("https://bulks-faostat.fao.org/production/"
            "Production_Crops_Livestock_E_All_Data_(Normalized).zip")
CSV_NAME = "Production_Crops_Livestock_E_All_Data_(Normalized).csv"
ELEMENTS = {5312: "area_harvested_ha", 5412: "yield_kg_ha", 5510: "production_t"}
FIRST_YEAR = 2000
data_source = "FAOSTAT, Crops and livestock products (QCL)"

local_file = os.environ.get("FAO_BULK_FILE")
if local_file and os.path.isfile(local_file):
    print(f"reading {local_file}")
    archive = zipfile.ZipFile(local_file)
else:
    print(f"downloading {BULK_URL}")
    response = http_get(BULK_URL, timeout=600)
    response.raise_for_status()
    archive = zipfile.ZipFile(io.BytesIO(response.content))

columns = ["Area", "Item Code", "Item Code (CPC)", "Item", "Element Code", "Year", "Value", "Flag"]
parts = []
with archive.open(CSV_NAME) as f:
    for chunk in pd.read_csv(f, encoding="utf-8", usecols=columns, chunksize=500_000):
        group_total = chunk["Item Code (CPC)"].str.lstrip("'").str.startswith("F")
        keep = (chunk["Element Code"].isin(list(ELEMENTS)) & (chunk["Year"] >= FIRST_YEAR)
                & ~group_total)
        parts.append(chunk[keep])
long = pd.concat(parts, ignore_index=True)
crop_items = long.loc[long["Element Code"] == 5312, "Item Code"].unique()
long = long[long["Item Code"].isin(crop_items)]
long["element"] = long["Element Code"].map(ELEMENTS)

keys = ["Area", "Item Code", "Item", "Year"]
values = long.pivot_table(index=keys, columns="element", values="Value", aggfunc="first")
production_flag = (long[long["Element Code"] == 5510]
                   .set_index(keys)["Flag"].rename("production_flag"))
crops = values.join(production_flag).reset_index().rename(columns={
    "Area": "fao_area", "Item Code": "item_code", "Item": "item", "Year": "year"})

# COMMAND ----------

country = read_table("country", columns=["country_name", "country_code"])
df = crops.merge(country, left_on="fao_area", right_on="country_name", how="inner")
unmatched = sorted(set(crops["fao_area"]) - set(df["fao_area"]))
print(f"{len(unmatched)} FAO areas (regions included) match none of the {len(country)} countries "
      f"of the country table by name and are left out; first ones: {unmatched[:20]}")

df["data_source"] = data_source
df = df[["country_name", "country_code", "year", "item_code", "item",
         "area_harvested_ha", "yield_kg_ha", "production_t", "production_flag", "data_source"]]
df

# COMMAND ----------

write_table(df, "fao_crop_production")
