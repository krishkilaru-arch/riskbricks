# Databricks notebook source
# MAGIC %md
# MAGIC # 📐 Factor Exposure Agent (Barra-like)
# MAGIC
# MAGIC Runs the FF3 factor model to approximate Barra-style factor exposures.
# MAGIC Output: `riskbricks.gold.risk_factor_exposures`

# COMMAND ----------

dbutils.widgets.text("start_date", "2025-01-01", "Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End date (YYYY-MM-DD)")

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import io
import zipfile
import numpy as np
import pandas as pd
import requests
from pyspark.sql import functions as F

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")

local_tz = ZoneInfo("America/New_York")
start_date = dbutils.widgets.get("start_date").strip()
end_date = dbutils.widgets.get("end_date").strip()
if not end_date:
    end_date = (datetime.now(local_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")

print(f"✅ Factor exposure window: {start_date} → {end_date}")

def fetch_ff3(start_dt: str, end_dt: str) -> pd.DataFrame:
    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as handle:
            raw = handle.read().decode("utf-8", errors="ignore")
    lines = raw.splitlines()
    start_idx = next(i for i, line in enumerate(lines) if line.strip().startswith(",Mkt-RF"))
    ff_df = pd.read_csv(io.StringIO("\n".join(lines[start_idx:])), engine="python")
    ff_df = ff_df.rename(columns={"Mkt-RF": "mkt_rf", "RF": "rf"})
    ff_df["date"] = pd.to_datetime(ff_df.iloc[:, 0], format="%Y%m%d", errors="coerce")
    ff_df = ff_df.dropna(subset=["date"]).drop(columns=[ff_df.columns[0]])
    ff_df = ff_df.set_index("date")
    ff_df = ff_df.apply(pd.to_numeric, errors="coerce") / 100.0
    return ff_df.loc[start_dt:end_dt]

price_table = "riskbricks.silver.stock_prices" if spark.catalog.tableExists("riskbricks.silver.stock_prices") else "riskbricks.bronze.stock_prices_bronze"
prices = spark.table(price_table) \
    .select("symbol", "date", "close") \
    .filter((F.col("date") >= F.lit(start_date)) & (F.col("date") <= F.lit(end_date)))

symbols_df = spark.table("riskbricks.gold.company_universe").select("symbol").distinct()
prices = prices.join(symbols_df, "symbol", "inner")

pdf = prices.toPandas()
if pdf.empty:
    raise ValueError("No price data found for the selected range.")

ff3 = fetch_ff3(start_date, end_date)

pdf["date"] = pd.to_datetime(pdf["date"])
pdf = pdf.sort_values(["symbol", "date"])

results = []
for symbol, group in pdf.groupby("symbol"):
    group = group.set_index("date")
    returns = np.log(group["close"] / group["close"].shift(1)).dropna()
    df = pd.concat([returns, ff3], axis=1).dropna()
    if df.empty:
        continue
    y = (df.iloc[:, 0] - df["rf"]).values
    X = df[["mkt_rf", "SMB", "HML"]].values
    X = np.column_stack([np.ones(len(X)), X])
    coeffs, *_ = np.linalg.lstsq(X, y, rcond=None)
    alpha = float(coeffs[0])
    betas = coeffs[1:]
    residuals = y - (alpha + X[:, 1:] @ betas)
    idio_var = float(np.var(residuals, ddof=1))
    factor_cov = np.cov(df[["mkt_rf", "SMB", "HML"]].values.T, ddof=1)
    factor_var = float(betas.T @ factor_cov @ betas)
    total_var = factor_var + idio_var
    total_vol = float(np.sqrt(total_var))
    annualized_vol = float(np.sqrt(total_var * 252))

    results.append({
        "symbol": symbol,
        "model": "ff3",
        "alpha": alpha,
        "beta_mkt": float(betas[0]),
        "beta_smb": float(betas[1]),
        "beta_hml": float(betas[2]),
        "factor_var": factor_var,
        "idio_var": idio_var,
        "total_var": total_var,
        "total_vol": total_vol,
        "annualized_vol": annualized_vol,
        "start_date": start_date,
        "end_date": end_date,
        "computed_at": datetime.utcnow()
    })

result_df = spark.createDataFrame(pd.DataFrame(results))

table_name = f"{catalog}.gold.risk_factor_exposures"
if not spark.catalog.tableExists(table_name):
    result_df.write \
        .mode("overwrite") \
        .partitionBy("model") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
else:
    replace_where = f"model = 'ff3'"
    result_df.write \
        .mode("overwrite") \
        .option("replaceWhere", replace_where) \
        .saveAsTable(table_name)

print(f"✅ Saved FF3 exposures to {table_name}")

# COMMAND ----------

dbutils.notebook.exit("✅ Factor exposure agent complete")
