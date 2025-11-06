from fastapi import FastAPI, Request, UploadFile, Form
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
import pandas as pd
import os

app = FastAPI(title="GLS Molina 2003 Recogidas")

# Carpetas de recursos
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Base de datos
DB_PATH = "data.db"
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)

# Crear tabla si no existe
with engine.begin() as conn:
    conn.exec_driver_sql("""
    CREATE TABLE IF NOT EXISTS pickups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT,
        direccion TEXT,
        poblacion TEXT,
        cp TEXT,
        cod_repartidor TEXT,
        nombre_repartidor TEXT
    );
    """)


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "active": "buscar"})


@app.get("/todas", response_class=HTMLResponse)
def todas(request: Request):
    with engine.begin() as conn:
        res = conn.execute(text("""
        SELECT 
            nombre,
            direccion,
            poblacion,
            cp,
            cod_repartidor,
            nombre_repartidor,
            COUNT(*) as total
        FROM pickups
        GROUP BY nombre, direccion, poblacion, cp, cod_repartidor, nombre_repartidor
        ORDER BY nombre_repartidor, direccion
        """))
        datos = [dict(r._mapping) for r in res.fetchall()]
    return templates.TemplateResponse("todas.html", {"request": request, "rows": datos, "total": len(datos), "active": "todas"})


@app.post("/api/upload")
async def upload_excel(file: UploadFile):
    try:
        df = pd.read_excel(file.file)
        required_cols = ["M", "N", "O", "P", "AT", "AU"]
        if len(df.columns) < 47:
            raise Exception("El Excel no tiene las columnas necesarias (M, N, O, P, AT, AU)")

        cols = ["nombre", "direccion", "poblacion", "cp", "cod_repartidor", "nombre_repartidor"]
        df = df.iloc[:, [12, 13, 14, 15, 45, 46]]
        df.columns = cols

        df = df.dropna(subset=["nombre", "direccion"])
        df = df.astype(str)

        with engine.begin() as conn:
            df.to_sql("pickups", conn, if_exists="append", index=False)

        return JSONResponse({"ok": True, "inserted": len(df)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get("/api/search")
def search(q: str):
    q_like = f"%{q}%"
    with engine.begin() as conn:
        res = conn.execute(text("""
        SELECT * FROM pickups 
        WHERE direccion LIKE :q OR nombre LIKE :q OR poblacion LIKE :q OR cp LIKE :q
        GROUP BY direccion, nombre_repartidor
        """), {"q": q_like})
        data = [dict(r._mapping) for r in res.fetchall()]
    return JSONResponse(data)


@app.post("/api/clear")
def clear():
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pickups"))
    return JSONResponse({"ok": True})


@app.get("/api/export")
def export():
    with engine.begin() as conn:
        df = pd.read_sql("SELECT * FROM pickups", conn)
    export_path = "export.csv"
    df.to_csv(export_path, index=False)
    return FileResponse(export_path, filename="recogidas_export.csv", media_type="text/csv")
