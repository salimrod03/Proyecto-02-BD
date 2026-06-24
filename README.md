# Agentic Analytics — Retail TPC-DS
**BigData 2026A | UNSA — Escuela de Ciencias de la Computación**

Sistema de análisis conversacional sobre datos TPC-DS (scale 10) en AWS EMR.
El usuario escribe una pregunta en español → Gemini genera el HiveQL/SparkSQL → se ejecuta en tiempo real sobre Hive/Spark en EMR → resultados reales en pantalla.

---

## Arquitectura

```
Browser (agentic_analytics.html)
    │
    ├─► Gemini 2.5 Flash API  →  genera SQL
    │
    └─► Flask API (EMR master :5000)  →  Beeline → HiveServer2 → datos en S3
```

**Datos en S3:** `s3://datos-lab-mel/tpcds/raw/`  
Los datos NO se pierden al eliminar el cluster. Solo hay que recrear las tablas.

---

## Requisitos previos

- Archivo `hadoop_clave.pem` (clave PEM del key pair configurado en EMR)
- Acceso a la consola AWS (para crear el cluster y abrir el puerto 5000)
- Gemini API Key (Google AI Studio)
- Bucket S3 `datos-lab-mel` con los datos ya cargados (subcarpetas: `customer/`, `item/`, `store/`, `date_dim/`, `store_sales/`)

---

## Paso 1 — Crear el cluster EMR

En la consola AWS → EMR → Create cluster, usar esta configuración:

| Campo | Valor |
|-------|-------|
| Versión EMR | emr-7.x (con Hive y Spark) |
| Nodo Principal | m5.xlarge × 1 |
| Nodo Central | m5.xlarge × 1 |
| Nodos Tarea | m5.xlarge × 3 |
| EBS por nodo | 75 GiB |
| Key pair | `hadoop_clave` (el que genera el .pem) |

Esperar a que el cluster esté en estado **Waiting**.

---

## Paso 2 — Abrir el puerto 5000 en el Security Group

1. Ir a **EC2 → Security Groups**
2. Buscar el grupo `ElasticMapReduce-master`
3. **Inbound rules → Edit → Add rule:**
   - Type: Custom TCP
   - Port: `5000`
   - Source: `0.0.0.0/0` (o tu IP específica para mayor seguridad)
4. Guardar

---

## Paso 3 — Obtener la IP pública del nodo principal

En la consola EMR → clic en el cluster → pestaña **Summary** → copiar el valor de **Master public DNS**, que tiene la forma:

```
ec2-XX-XX-XX-XX.compute-1.amazonaws.com
```

La IP pública equivalente es `XX.XX.XX.XX` (los números del DNS).

---

## Paso 4 — Conectarse al cluster

```bash
chmod 400 hadoop_clave.pem

ssh -i hadoop_clave.pem hadoop@ec2-XX-XX-XX-XX.compute-1.amazonaws.com
```

---

## Paso 5 — Recrear la base de datos Hive en HiveServer2

> **Importante:** cada vez que se crea un cluster nuevo, el metastore de HiveServer2 está vacío. Los datos en S3 siguen intactos; solo hay que registrar las tablas de nuevo. Usar **beeline** (no el comando `hive`), ya que el API Flask se conecta a HiveServer2.

Ejecutar el siguiente bloque completo dentro del cluster:

```bash
beeline -u "jdbc:hive2://localhost:10000" -n hadoop << 'EOF'
CREATE DATABASE IF NOT EXISTS tpcds;
USE tpcds;

CREATE EXTERNAL TABLE IF NOT EXISTS customer (
  c_customer_sk INT, c_customer_id STRING, c_current_cdemo_sk INT,
  c_current_hdemo_sk INT, c_current_addr_sk INT, c_first_shipto_date_sk INT,
  c_first_sales_date_sk INT, c_salutation STRING, c_first_name STRING,
  c_last_name STRING, c_preferred_cust_flag STRING, c_birth_day INT,
  c_birth_month INT, c_birth_year INT, c_birth_country STRING,
  c_login STRING, c_email_address STRING, c_last_review_date STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION 's3://datos-lab-mel/tpcds/raw/customer/';

CREATE EXTERNAL TABLE IF NOT EXISTS item (
  i_item_sk INT, i_item_id STRING, i_rec_start_date STRING, i_rec_end_date STRING,
  i_item_desc STRING, i_current_price DOUBLE, i_wholesale_cost DOUBLE,
  i_brand_id INT, i_brand STRING, i_class_id INT, i_class STRING,
  i_category_id INT, i_category STRING, i_manufact_id INT, i_manufact STRING,
  i_size STRING, i_formulation STRING, i_color STRING, i_units STRING,
  i_container STRING, i_manager_id INT, i_product_name STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION 's3://datos-lab-mel/tpcds/raw/item/';

CREATE EXTERNAL TABLE IF NOT EXISTS store (
  s_store_sk INT, s_store_id STRING, s_rec_start_date STRING, s_rec_end_date STRING,
  s_closed_date_sk INT, s_store_name STRING, s_number_employees INT,
  s_floor_space INT, s_hours STRING, s_manager STRING, s_market_id INT,
  s_geography_class STRING, s_market_desc STRING, s_market_manager STRING,
  s_division_id INT, s_division_name STRING, s_company_id INT, s_company_name STRING,
  s_street_number STRING, s_street_name STRING, s_street_type STRING,
  s_suite_number STRING, s_city STRING, s_county STRING, s_state STRING,
  s_zip STRING, s_country STRING, s_gmt_offset DOUBLE, s_tax_precentage DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION 's3://datos-lab-mel/tpcds/raw/store/';

CREATE EXTERNAL TABLE IF NOT EXISTS date_dim (
  d_date_sk INT, d_date_id STRING, d_date STRING, d_month_seq INT,
  d_week_seq INT, d_quarter_seq INT, d_year INT, d_dow INT, d_moy INT,
  d_dom INT, d_qoy INT, d_fy_year INT, d_fy_quarter_seq INT, d_fy_week_seq INT,
  d_day_name STRING, d_quarter_name STRING, d_holiday STRING, d_weekend STRING,
  d_following_holiday STRING, d_first_dom INT, d_last_dom INT,
  d_same_day_ly INT, d_same_day_lq INT, d_current_day STRING,
  d_current_week STRING, d_current_month STRING, d_current_quarter STRING,
  d_current_year STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION 's3://datos-lab-mel/tpcds/raw/date_dim/';

CREATE EXTERNAL TABLE IF NOT EXISTS store_sales (
  ss_sold_date_sk INT, ss_sold_time_sk INT, ss_item_sk INT, ss_customer_sk INT,
  ss_cdemo_sk INT, ss_hdemo_sk INT, ss_addr_sk INT, ss_store_sk INT,
  ss_promo_sk INT, ss_ticket_number BIGINT, ss_quantity INT,
  ss_wholesale_cost DOUBLE, ss_list_price DOUBLE, ss_sales_price DOUBLE,
  ss_ext_discount_amt DOUBLE, ss_ext_sales_price DOUBLE,
  ss_ext_wholesale_cost DOUBLE, ss_ext_list_price DOUBLE, ss_ext_tax DOUBLE,
  ss_coupon_amt DOUBLE, ss_net_paid DOUBLE, ss_net_paid_inc_tax DOUBLE,
  ss_net_profit DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION 's3://datos-lab-mel/tpcds/raw/store_sales/';

SHOW TABLES;
EOF
```

Debe mostrar al final:
```
customer
date_dim
item
store
store_sales
```

---

## Paso 5b — (Opcional) Crear tablas Parquet para el motor `spark_opt`

El backend soporta tres motores: `hive`, `spark` y `spark_opt`. El motor
**`spark_opt`** ejecuta Spark SQL con optimizaciones (AQE, coalesce de
particiones, skew join, broadcast join, filter pushdown) sobre una copia de las
tablas en **formato Parquet** en la base de datos `tpcds_opt`. El backend
reescribe automáticamente el SQL generado sobre `tpcds` hacia `tpcds_opt`, así
que el usuario y Gemini siguen consultando las tablas normales.

> Solo es necesario si vas a usar el botón **⚡ Opt** del frontend. Si no creas
> estas tablas, `hive` y `spark` siguen funcionando igual.

Dentro del cluster, generar y ejecutar el script de creación:

```bash
cat > setup_spark_optimized.sql <<'SQL'
CREATE DATABASE IF NOT EXISTS tpcds_opt;

CREATE TABLE IF NOT EXISTS tpcds_opt.customer_parquet
USING PARQUET LOCATION 's3://datos-lab-mel/tpcds/parquet/customer'
AS SELECT * FROM tpcds.customer;

CREATE TABLE IF NOT EXISTS tpcds_opt.item_parquet
USING PARQUET LOCATION 's3://datos-lab-mel/tpcds/parquet/item'
AS SELECT * FROM tpcds.item;

CREATE TABLE IF NOT EXISTS tpcds_opt.store_parquet
USING PARQUET LOCATION 's3://datos-lab-mel/tpcds/parquet/store'
AS SELECT * FROM tpcds.store;

CREATE TABLE IF NOT EXISTS tpcds_opt.date_dim_parquet
USING PARQUET LOCATION 's3://datos-lab-mel/tpcds/parquet/date_dim'
AS SELECT * FROM tpcds.date_dim;

CREATE TABLE IF NOT EXISTS tpcds_opt.store_sales_parquet
USING PARQUET LOCATION 's3://datos-lab-mel/tpcds/parquet/store_sales'
AS SELECT * FROM tpcds.store_sales;
SQL

spark-sql --master yarn --deploy-mode client \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=80 \
  -f setup_spark_optimized.sql
```

(Opcional) Calcular estadísticas para mejores planes de ejecución:

```bash
cat > analyze_tables.sql <<'SQL'
ANALYZE TABLE tpcds_opt.customer_parquet    COMPUTE STATISTICS;
ANALYZE TABLE tpcds_opt.item_parquet        COMPUTE STATISTICS;
ANALYZE TABLE tpcds_opt.store_parquet       COMPUTE STATISTICS;
ANALYZE TABLE tpcds_opt.date_dim_parquet    COMPUTE STATISTICS;
ANALYZE TABLE tpcds_opt.store_sales_parquet COMPUTE STATISTICS;
SQL

spark-sql --master yarn --deploy-mode client \
  --conf spark.sql.catalogImplementation=hive \
  -f analyze_tables.sql
```

Verificar:

```bash
spark-sql --conf spark.sql.catalogImplementation=hive \
  -e "SHOW TABLES IN tpcds_opt;"
```

Probar el motor optimizado desde local (el backend devuelve también `sql_used`):

```bash
curl -X POST http://XX.XX.XX.XX:5000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT COUNT(*) FROM store_sales","engine":"spark_opt"}'
```

---

## Paso 6 — Desplegar el backend Flask

Desde tu máquina local, copiar el archivo al cluster:

```bash
scp -i hadoop_clave.pem \
  api_emr.py \
  hadoop@ec2-XX-XX-XX-XX.compute-1.amazonaws.com:~/
```

Conectarse al cluster e instalar dependencias:

```bash
ssh -i hadoop_clave.pem hadoop@ec2-XX-XX-XX-XX.compute-1.amazonaws.com

pip install flask flask-cors
```

Lanzar el servidor con `screen` para que persista al cerrar el SSH:

```bash
screen -S api
python3 api_emr.py
```

Cuando aparezca `Running on http://0.0.0.0:5000`, presionar **Ctrl+A** luego **D** para desconectarse del screen sin matarlo.

Verificar que funciona:

```bash
curl http://localhost:5000/health
# Esperado: {"database":"tpcds","status":"ok"}
```

---

## Paso 7 — Actualizar la IP en el HTML

Abrir `agentic_analytics.html` en un editor y cambiar la línea 431:

```js
// Cambiar XX.XX.XX.XX por la nueva IP pública del cluster
const EMR_API_URL = "http://XX.XX.XX.XX:5000/query";
```

---

## Paso 8 — Verificar la conexión desde local

```bash
# Health check
curl http://XX.XX.XX.XX:5000/health

# Consulta real (tarda ~30-50s)
curl -X POST http://XX.XX.XX.XX:5000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT COUNT(*) FROM store","engine":"hive"}'

# Respuesta esperada:
# {"columns":["_c0"],"rows":[["102"]],"exec_time":5.2,...}
```

---

## Paso 9 — Abrir el HTML en el navegador

```bash
xdg-open agentic_analytics.html
# o doble click en el explorador de archivos
```

---

## Checklist de inicio rápido

Al crear un cluster nuevo, este es el orden mínimo:

- [ ] Cluster EMR en estado **Waiting**
- [ ] Puerto 5000 abierto en Security Group de `ElasticMapReduce-master`
- [ ] Tablas recreadas en HiveServer2 via beeline (Paso 5)
- [ ] `api_emr.py` copiado y corriendo con `screen` (Paso 6)
- [ ] IP actualizada en `agentic_analytics.html` línea 431 (Paso 7)
- [ ] Health check responde OK (Paso 8)

---

## Datos del dataset (TPC-DS scale 10)

| Tabla | Registros |
|-------|-----------|
| store_sales | 28,800,991 |
| customer | 500,000 |
| item | 102,000 |
| date_dim | 73,049 |
| store | 102 |

**Ubicación S3:** `s3://datos-lab-mel/tpcds/raw/`  
**Tamaño total:** ~11.2 GB, 44 archivos

---

## Tiempos de referencia en Hive (cluster m5.xlarge)

| Consulta | Tiempo aprox. |
|----------|--------------|
| COUNT(*) store | ~5s |
| COUNT(*) store_sales | ~48s |
| TOP 20 clientes | ~30s |
| Ventas por tienda | ~28s |
| Ventas por mes (JOIN) | ~29s |
| Ventas por día (JOIN) | ~29s |
| Top productos por tienda | ~50s |

---

## Solución de problemas frecuentes

**`Database does not exist: tpcds`**  
→ Repetir el Paso 5 (recrear tablas vía beeline). Ocurre en cada cluster nuevo.

**`Failed to connect to 3.89.19.55 port 5000`**  
→ Verificar que el proceso Flask sigue corriendo: `ssh ... "ps aux | grep api_emr"`  
→ Si no está: `screen -r api` o relanzar con `screen -S api && python3 api_emr.py`  
→ Verificar que el puerto 5000 está en las reglas de entrada del Security Group.

**`curl: (7) Failed to connect`**  
→ La IP cambió al recrear el cluster. Actualizar en `agentic_analytics.html` línea 431.

**Proceso Flask muere al cerrar SSH**  
→ Usar siempre `screen`. Para reconectarse a una sesión existente: `screen -r api`

**Beeline se conecta pero `USE tpcds` falla**  
→ No usar el comando `hive -e` para crear tablas; siempre usar `beeline -u jdbc:hive2://localhost:10000`.
