import re
import sqlite3
import requests
import pandas as pd
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt

URL = "https://es.wikipedia.org/wiki/Anexo:Películas_con_las_mayores_recaudaciones"
HEADERS = {"User-Agent": "Mozilla/5.0"}  # Evitar bloqueos impersonando el navegador Mozilla.
DB_PATH = "box_office.db"
TABLE_NAME = "peliculas_mas_taquilleras"

def to_number(x):
    """Convierte texto con símbolos de moneda a número entero."""
    if pd.isna(x):
        return pd.NA
    s = str(x)
    s = re.sub(r"[^\d]", "", s)  # deja solo dígitos
    return pd.to_numeric(s, errors="coerce")

def main():
    # descargar y parsear la tabla objetivo
    resp = requests.get(URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    target = None
    for t in soup.select("table.wikitable"):
        cap = t.find("caption")
        if cap and "mayores recaudaciones a nivel mundial" in cap.get_text(strip=True).lower() and "usd" in cap.get_text(strip=True).lower():
            target = t
            break
    if target is None:
        raise RuntimeError("No encontré la tabla.")

    df = pd.read_html(str(target), flavor="bs4")[0]

    # limpieza y detección de tipos de columnas.
    for col in df.columns:
        if "recaud" in str(col).lower() or "taquilla" in str(col).lower():
            df[col] = df[col].apply(to_number)

    for name in df.columns:
        ln = str(name).lower()
        if ln in ("n.º", "nº", "n°", "puesto"):
            df[name] = pd.to_numeric(df[name], errors="coerce", downcast="integer")
        if "año" in ln and "estreno" in ln:
            df[name] = pd.to_numeric(df[name], errors="coerce", downcast="integer")

    # exportar a sql lite.
    with sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES) as con:
        con.text_factory = lambda x: str(x, "utf-8", "ignore")
        df.to_sql(TABLE_NAME, con, if_exists="replace", index=False)

    print(f"Filas cargadas: {len(df)}  | DB: {DB_PATH}  | Tabla: {TABLE_NAME}")

    # visualizaciones
    plt.style.use("ggplot")

    # Top 10 películas más taquilleras (Recaudación mundial)
    col_world = [c for c in df.columns if "mundial" in c.lower()][0]
    col_movie = [c for c in df.columns if "película" in c.lower()][0]

    top10 = df.nlargest(10, col_world)[[col_movie, col_world]].set_index(col_movie)
    top10[col_world] = top10[col_world] / 1e9  # convertir a miles de millones USD
    top10.plot(kind="barh", figsize=(10, 6), color="steelblue")
    plt.title("Top 10 películas más taquilleras (en miles de millones USD)")
    plt.xlabel("Recaudación mundial (B USD)")
    plt.ylabel("")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.show()

    # Recaudación mundial media por año
    if any("año" in c.lower() for c in df.columns):
        col_year = [c for c in df.columns if "año" in c.lower()][0]
        yearly = df.groupby(col_year)[col_world].mean().dropna() / 1e9
        yearly.plot(kind="line", marker="o", figsize=(10, 5))
        plt.title("Evolución de la recaudación media por año de estreno")
        plt.ylabel("Media recaudación (B USD)")
        plt.xlabel("Año de estreno")
        plt.tight_layout()
        plt.show()

    # Analizar la taquilla doméstica vs internacional
    domestic_col = [c for c in df.columns if "ee. uu" in c.lower()]
    foreign_col = [c for c in df.columns if "fuera" in c.lower()]
    if domestic_col and foreign_col:
        comp = df.nlargest(10, col_world)[[col_movie, domestic_col[0], foreign_col[0]]]
        comp = comp.set_index(col_movie) / 1e9
        comp.plot(kind="barh", stacked=True, figsize=(10, 6))
        plt.title("Top 10: taquilla EE. UU. vs fuera de EE. UU. (B USD)")
        plt.xlabel("Recaudación total (B USD)")
        plt.ylabel("")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.show()

if __name__ == "__main__":
    main()
