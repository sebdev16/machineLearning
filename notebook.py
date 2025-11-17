#primera celda de notebook.py
#esta es para conectarse a la api y que este en segundo plano
import subprocess, sys

server = subprocess.Popen([
    sys.executable, "-m", "uvicorn", "main:app",
    "--host", "127.0.0.1", "--port", "8000"
])

print("Servidor iniciado en http://127.0.0.1:8000")

#------------------------------------------------------------------------
#esta es para cerrar el servidor cuando se cierre el notebook
server.terminate()
print("Servidor detenido.")

#-------------------------------------------------------------------------
#verificamos que el servidor este corriendo
import requests
requests.get("http://127.0.0.1:8000/health").json()
#--------------------------------------------------------------------------
#Comprueba que el archivo CSV existe, se puede abrir y tiene las columnas esperadas
import pandas as pd

pd.read_csv("Fleet_Work_Orders_20251111.csv").head()
#--------------------------------------------------------------------------
#muestra la info para data set 
df = pd.read_csv("Fleet_Work_Orders_20251111.csv")
df.head()
#--------------------------------------------------------------------------
#Luego revisa la información general
df.info()
#--------------------------------------------------------------------------
#Y revisa valores faltantes
df.isna().sum()
#--------------------------------------------------------------------------
#Convertir columnas de fecha a tipo datetime
# errors="coerce" convierte fechas inválidas en NaT en vez de romper el código.
date_cols = [
    "In Service Date",
    "Work Order Begin Date",
    "Work Order Finish Date"
]

for c in date_cols:
    df[c] = pd.to_datetime(df[c], errors="coerce")
#--------------------------------------------------------------------------
#Convertir columnas de fecha a tipo datetime
# errors="coerce" convierte fechas inválidas en NaT en vez de romper el código.
date_cols = [
    "In Service Date",
    "Work Order Begin Date",
    "Work Order Finish Date"
]

for c in date_cols:
    df[c] = pd.to_datetime(df[c], errors="coerce")
#--------------------------------------------------------------------------
#eliminar duplicados si los hay
df = df.drop_duplicates()
#--------------------------------------------------------------------------
# manejo de valores faltantes
df = df.fillna({
    "Total Cost": 0,
    "Expected Hours": 0,
    "Actual Hours": 0,
    "Days to Complete": 0,
    "Vehicle Make": "UNKNOWN",
    "Vehicle Model": "UNKNOWN",
    "WO Reason": "UNKNOWN"
})
#--------------------------------------------------------------------------
#mostramos
df.info()
df.head()
df.describe(include="all")
#--------------------------------------------------------------------------