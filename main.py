from fastapi import FastAPI
from fastapi import Query, HTTPException
import pandas as pd
import numpy as np

def json_safe_records(df_):
    # Reemplaza ±inf por NA y luego NaN/NA por None (JSON válido)
    tmp = df_.replace([np.inf, -np.inf], pd.NA)
    tmp = tmp.where(pd.notnull(tmp), None)
    return tmp.to_dict(orient="records")

CSV_PATH = "Fleet_Work_Orders_20251111.csv"
app = FastAPI(title="Mantenimiento coches API", version="0.1.0")

COLS = [
    "Work Order Number",
    "Vehicle Number",
    "Total Cost",
    "Vehicle Make",
    "Vehicle Model",
    "Vehicle Year",
    "In Service Date",
    "Work Order Begin Date",
    "Work Order Finish Date",
    "Expected Hours",
    "Actual Hours",
    "Days to Complete",
    "WO Reason",
    "On Time Indicator",
    "Open Indicator",
    "WO Vehicle Odometer"
]
df_raw = pd.read_csv(CSV_PATH, usecols=lambda c: c in COLS, low_memory=False)

@app.get("/ordenes/count")
def numero_ordenes():
    return {"rows": int(len(df_raw))}


@app.get("/health")
def health():
    return {"Status": "Ok"}

from fastapi.responses import StreamingResponse

@app.get("/export/csv")
def export_csv(
    cols: str = Query(..., description="Lista separada por comas de columnas EXACTAS a incluir"),
    chunksize: int = Query(100_000, ge=1, le=1_000_000),
    filename: str = "work_orders_raw.csv",
):
    data = df_raw

    # Validar y aplicar columnas solicitadas (en ese orden)
    requested = [c.strip() for c in cols.split(",") if c.strip()]
    available = list(df_raw.columns)
    invalid = [c for c in requested if c not in available]
    if invalid or not requested:
        raise HTTPException(
            400,
            {"error": "Columnas inválidas o vacías en 'cols'.", "invalid": invalid, "available": available},
        )
    data = data[requested]

    def iter_csv(df_, chunk=chunksize):
        n = len(df_)
        if n == 0:
            yield ",".join(df_.columns) + "\n"
            return
        for i in range(0, n, chunk):
            part = df_.iloc[i:i+chunk]
            yield part.to_csv(index=False, header=(i == 0))

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(iter_csv(data), media_type="text/csv", headers=headers)