#!/usr/bin/env python3
"""
Backend Flask para Agentic Analytics — ejecuta SQL real en Hive o Spark SQL
sobre el Data Warehouse TPC-DS en S3.

Despliega en el nodo principal del EMR:
    pip install flask flask-cors
    python api_emr.py
"""

import subprocess
import time
import re
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)          # permite llamadas desde el browser (cualquier origen)

DATABASE      = "tpcds"
DATABASE_OPT  = "tpcds_opt"   # base de datos con tablas Parquet optimizadas
HIVE_TIMEOUT  = 300   # segundos máximo por consulta Hive
SPARK_TIMEOUT = 300   # segundos máximo por consulta Spark
METRICS_FILE  = "query_metrics.csv"


# ── helpers ──────────────────────────────────────────────────────────────────

def parse_tsv(raw: str):
    """
    Convierte la salida TSV de hive/spark-sql en columns + rows.
    Descarta líneas vacías y líneas que son solo guiones (separadores).
    """
    lines = [l for l in raw.strip().splitlines()
             if l.strip() and not re.match(r'^[-\t]+$', l)]
    if not lines:
        return [], []

    columns = [c.strip() for c in lines[0].split('\t')]
    rows = []
    for line in lines[1:]:
        cells = [c.strip() for c in line.split('\t')]
        # rellenar si hay menos celdas que columnas
        while len(cells) < len(columns):
            cells.append('')
        rows.append(cells[:len(columns)])

    return columns, rows


def run_hive(sql: str):
    """Ejecuta SQL via Beeline → HiveServer2 (así funciona Hive en EMR)."""
    start = time.time()
    proc = subprocess.run(
        [
            "beeline",
            "-u", "jdbc:hive2://localhost:10000",
            "-n", "hadoop",
            "--outputformat=tsv2",
            "--silent=true",
            "--fastConnect=true",
            "-e", f"USE {DATABASE}; {sql}"
        ],
        capture_output=True, text=True, timeout=HIVE_TIMEOUT
    )
    elapsed = round(time.time() - start, 2)

    # Beeline devuelve errores en stderr; stdout puede tener advertencias mezcladas
    stderr_clean = proc.stderr.strip() if proc.stderr else ""
    if proc.returncode != 0 or ("Error" in stderr_clean and not proc.stdout.strip()):
        return {"error": stderr_clean[-1000:] or "Error desconocido", "exec_time": elapsed}

    columns, rows = parse_tsv(proc.stdout)
    rows_count = estimate_rows_scanned(sql)
    return {
        "columns": columns,
        "rows": rows,
        "exec_time": elapsed,
        "rows_scanned": rows_count,
        "engine": "hive",
        "error": None
    }


def run_spark(sql: str):
    """Ejecuta SQL en Spark SQL usando el metastore Hive del EMR."""
    full_sql = f"USE {DATABASE}; {sql}"
    start = time.time()
    proc = subprocess.run(
        ["spark-sql",
         "--conf", "spark.hadoop.hive.metastore.uris=thrift://localhost:9083",
         "--conf", "spark.sql.catalogImplementation=hive",
         "-e", full_sql],
        capture_output=True, text=True, timeout=SPARK_TIMEOUT
    )
    elapsed = round(time.time() - start, 2)

    if proc.returncode != 0:
        err_msg = proc.stderr[-1000:] if proc.stderr else "Error desconocido"
        return {"error": err_msg, "exec_time": elapsed}

    # Spark SQL mezcla logs con resultados en stdout; tomamos solo las líneas
    # que no empiezan con timestamp de log (ej. "24/01/01 12:00:00 INFO ...")
    log_pattern = re.compile(r'^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ')
    clean = "\n".join(
        l for l in proc.stdout.splitlines()
        if not log_pattern.match(l)
    )

    columns, rows = parse_tsv(clean)
    rows_count = estimate_rows_scanned(sql)
    return {
        "columns": columns,
        "rows": rows,
        "exec_time": elapsed,
        "rows_scanned": rows_count,
        "engine": "spark",
        "error": None
    }


def rewrite_sql_to_parquet(sql: str) -> str:
    """
    Reescribe las tablas normales (tpcds.*) hacia su versión Parquet
    optimizada (tpcds_opt.*_parquet). Así el usuario o Gemini pueden seguir
    generando SQL sobre las tablas originales y el backend lo redirige solo.
    El orden importa: store_sales antes que store para no romper el reemplazo.
    """
    replacements = [
        (r'\btpcds\.store_sales\b', 'tpcds_opt.store_sales_parquet'),
        (r'\btpcds\.customer\b',    'tpcds_opt.customer_parquet'),
        (r'\btpcds\.item\b',        'tpcds_opt.item_parquet'),
        (r'\btpcds\.date_dim\b',    'tpcds_opt.date_dim_parquet'),
        (r'\btpcds\.store\b',       'tpcds_opt.store_parquet'),

        (r'\bstore_sales\b', 'store_sales_parquet'),
        (r'\bcustomer\b',    'customer_parquet'),
        (r'\bitem\b',        'item_parquet'),
        (r'\bdate_dim\b',    'date_dim_parquet'),
        (r'\bstore\b',       'store_parquet'),
    ]

    optimized_sql = sql
    for pattern, replacement in replacements:
        optimized_sql = re.sub(pattern, replacement, optimized_sql,
                               flags=re.IGNORECASE)
    return optimized_sql


def run_spark_optimized(sql: str):
    """
    Ejecuta Spark SQL con optimizaciones: tablas Parquet, AQE, coalesce de
    particiones, skew join, broadcast join y filter pushdown.
    """
    optimized_sql = rewrite_sql_to_parquet(sql)
    full_sql = f"USE {DATABASE_OPT}; {optimized_sql}"
    start = time.time()
    proc = subprocess.run(
        [
            "spark-sql",
            "--master", "yarn",
            "--deploy-mode", "client",

            "--conf", "spark.hadoop.hive.metastore.uris=thrift://localhost:9083",
            "--conf", "spark.sql.catalogImplementation=hive",

            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
            "--conf", "spark.sql.adaptive.skewJoin.enabled=true",
            "--conf", "spark.sql.shuffle.partitions=80",

            "--conf", "spark.sql.autoBroadcastJoinThreshold=104857600",
            "--conf", "spark.sql.broadcastTimeout=600",

            "--conf", "spark.sql.parquet.filterPushdown=true",

            "-e", full_sql
        ],
        capture_output=True, text=True, timeout=SPARK_TIMEOUT
    )
    elapsed = round(time.time() - start, 2)

    if proc.returncode != 0:
        err_msg = proc.stderr[-1000:] if proc.stderr else "Error desconocido"
        return {
            "error": err_msg,
            "exec_time": elapsed,
            "engine": "spark_opt",
            "sql_original": sql,
            "sql_used": optimized_sql,
        }

    # Spark SQL mezcla logs con resultados en stdout; descartamos las líneas
    # que empiezan con timestamp de log (ej. "24/01/01 12:00:00 INFO ...")
    log_pattern = re.compile(r'^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ')
    clean = "\n".join(
        l for l in proc.stdout.splitlines()
        if not log_pattern.match(l)
    )

    columns, rows = parse_tsv(clean)
    rows_count = estimate_rows_scanned(sql)
    return {
        "columns": columns,
        "rows": rows,
        "exec_time": elapsed,
        "rows_scanned": rows_count,
        "engine": "spark_opt",
        "optimized": True,
        "sql_original": sql,
        "sql_used": optimized_sql,
        "error": None
    }


def ensure_limit(sql: str, default_limit: int = 100) -> str:
    """Exige LIMIT en los SELECT para no devolver demasiadas filas al browser."""
    sql_clean = sql.strip().rstrip(";")
    if re.search(r'\blimit\b', sql_clean, flags=re.IGNORECASE):
        return sql_clean + ";"
    if re.match(r'^\s*select\b', sql_clean, flags=re.IGNORECASE):
        return f"{sql_clean} LIMIT {default_limit};"
    return sql


def log_query(engine: str, sql: str, exec_time, error):
    """Guarda un historial de consultas para comparar motores."""
    try:
        with open(METRICS_FILE, "a") as f:
            safe_sql   = (sql or "").replace("\n", " ").replace(",", " ")
            safe_error = (error or "").replace("\n", " ").replace(",", " ")
            f.write(f"{time.time()},{engine},{exec_time},{safe_sql},{safe_error}\n")
    except Exception:
        pass   # las métricas no deben romper la respuesta


def estimate_rows_scanned(sql: str) -> str:
    """
    Estima filas escaneadas según las tablas mencionadas en el SQL.
    Valores reales del dataset TPC-DS scale-10 cargado en S3.
    """
    s = sql.lower()
    total = 0
    if "store_sales" in s:
        total += 28_800_991
    if "customer" in s:
        total += 500_000
    if "item" in s:
        total += 102_000
    if "store" in s and "store_sales" not in s:
        total += 102
    if "date_dim" in s:
        total += 73_049
    if total == 0:
        total = 28_800_991
    if total >= 1_000_000:
        return f"{total/1_000_000:.1f}M"
    if total >= 1_000:
        return f"{total/1_000:.1f}K"
    return str(total)


# ── endpoints ────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "database": DATABASE})


@app.route("/query", methods=["POST"])
def query():
    body = request.get_json(force=True)
    sql    = (body.get("sql") or "").strip()
    engine = (body.get("engine") or "hive").lower()

    if not sql:
        return jsonify({"error": "Campo 'sql' vacío"}), 400

    # Bloquea sentencias peligrosas (solo lectura)
    forbidden = re.compile(
        r'\b(drop|truncate|delete|insert|update|alter|create|load)\b',
        re.IGNORECASE
    )
    if forbidden.search(sql):
        return jsonify({"error": "Solo se permiten consultas SELECT"}), 403

    # Exige LIMIT para no devolver demasiadas filas al navegador
    sql = ensure_limit(sql)

    try:
        if engine == "spark":
            result = run_spark(sql)
        elif engine == "spark_opt":
            result = run_spark_optimized(sql)
        else:
            result = run_hive(sql)
    except subprocess.TimeoutExpired:
        return jsonify({"error": f"Tiempo de espera superado ({HIVE_TIMEOUT}s)"}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Registra métricas de ejecución para comparar motores
    log_query(
        engine=result.get("engine", engine),
        sql=sql,
        exec_time=result.get("exec_time"),
        error=result.get("error"),
    )

    return jsonify(result)


# ── main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Agentic Analytics — EMR Backend")
    print(f"  Base de datos: {DATABASE}")
    print("  Endpoints:")
    print("    GET  /health")
    print("    POST /query  { sql, engine: 'hive'|'spark'|'spark_opt' }")
    print("=" * 60)
    # host=0.0.0.0 para que sea accesible desde fuera del nodo
    app.run(host="0.0.0.0", port=5000, debug=False)
