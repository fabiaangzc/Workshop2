import os
import pymysql
import pandas as pd
import numpy as np

# --- CONFIGURACIÓN DE CONEXIÓN ---
connection = pymysql.connect(
    host="localhost",
    user="root",
    password="root",   # cambia esto
    database="raw",             # crea el schema antes si no existe,
    local_infile=True,
)

# --- CREAR TABLA SI NO EXISTE ---
create_table = """
CREATE TABLE IF NOT EXISTS grammys_raw (
  year INT,
  title VARCHAR(255),
  published_at VARCHAR(255),
  updated_at VARCHAR(255),
  category VARCHAR(255),
  nominee VARCHAR(255),
  artist VARCHAR(255),
  workers TEXT,
  img TEXT,
  winner TINYINT(1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

cursor = connection.cursor()
cursor.execute(create_table)
connection.commit()

# ruta al CSV (defínela antes para usarla en ambos paths)
csv_path = os.path.abspath(r"data/the_grammy_awards.csv")

# --- OPCIÓN 1: Cargar usando LOAD DATA LOCAL INFILE (rápido) ---
try:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV no encontrado en {csv_path}")

    # MySQL espera slashes en la ruta en algunos entornos
    infile_path = csv_path.replace('\\', '/')

    query = f"""
    LOAD DATA LOCAL INFILE '{infile_path}'
    INTO TABLE grammys_raw
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (year, title, published_at, updated_at, category, nominee, artist, workers, img, @winner)
    SET winner = CASE
        WHEN LOWER(TRIM(@winner)) IN ('1','true','t','yes','y') THEN 1
        WHEN LOWER(TRIM(@winner)) IN ('0','false','f','no','n') THEN 0
        ELSE NULL
    END;
    """

    cursor.execute(query)
    connection.commit()
    print("✅ Carga completa con LOAD DATA LOCAL INFILE")

except Exception as e:
    print("Error al cargar con LOAD DATA, intentando método alternativo...")
    print("Detalle:", repr(e))

    # --- OPCIÓN 2: (si LOCAL INFILE está deshabilitado) ---
    # Cargar con pandas y hacer inserts por lotes (más lento pero fiable)
    df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False)

    # Asegurar columnas en el orden esperado y rellenar columnas faltantes
    expected_cols = ['year', 'title', 'published_at', 'updated_at', 'category', 'nominee', 'artist', 'workers', 'img', 'winner']
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None

    df = df[expected_cols]

    # Normalizar winner a 0/1/None
    df['winner'] = df['winner'].apply(lambda x: 1 if str(x).strip().lower() in ('1','true','t','yes','y') else (0 if str(x).strip().lower() in ('0','false','f','no','n') else None))

    # Reemplazar NaN por None para que pymysql los convierta a NULL
    df = df.where(pd.notnull(df), None)

    insert_sql = """
        INSERT INTO grammys_raw (year,title,published_at,updated_at,category,nominee,artist,workers,img,winner)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    # Insertar en batches
    tuples = [tuple(x) for x in df.values]
    try:
        batch_size = 1000
        for i in range(0, len(tuples), batch_size):
            batch = tuples[i:i+batch_size]
            cursor.executemany(insert_sql, batch)
            connection.commit()
        print("✅ Carga completa usando pandas + executemany (batches)")
    except Exception as e2:
        print('Error durante inserts con pandas:', repr(e2))
        connection.rollback()

cursor.close()
connection.close()
