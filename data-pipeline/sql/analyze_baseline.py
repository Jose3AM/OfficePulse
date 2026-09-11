# ============================================================
# Librerías
# ============================================================

# Path nos permite manejar correctamente las rutas
# independientemente del sistema operativo.
from pathlib import Path
import re
import sys

# DuckDB será nuestro motor analítico SQL.
# Puede consultar archivos Parquet directamente sin necesidad
# de cargar los datos previamente a una base tradicional.
import duckdb

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


# ============================================================
# Configuración
# ============================================================

# Dataset generado por nuestro pipeline Polars.
PROCESSED_DIR = Path("data/processed")

# Solo se consideran archivos con el formato baseline_N.parquet.
BASELINE_FILENAME_PATTERN = re.compile(
    r"^baseline_([0-9]+)\.parquet$"
)


# ============================================================
# Validación del archivo
# ============================================================

def find_latest_baseline() -> Path:
    """
    Encuentra el baseline_N con el número más alto.

    El sufijo se convierte a entero para que baseline_10 se
    seleccione correctamente después de baseline_09.
    """

    candidates = []

    for path in PROCESSED_DIR.glob("baseline_*.parquet"):
        match = BASELINE_FILENAME_PATTERN.fullmatch(path.name)

        if match:
            candidates.append((int(match.group(1)), path))

    if not candidates:
        raise FileNotFoundError(
            "No se encontraron datasets con formato "
            f"baseline_N.parquet en {PROCESSED_DIR}"
        )

    return max(
        candidates,
        key=lambda candidate: candidate[0],
    )[1]


def validate_dataset(parquet_path: Path) -> None:
    """
    Comprueba que el archivo Parquet exista antes
    de intentar ejecutar consultas SQL.
    """

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"No existe el dataset: {parquet_path}"
        )


# ============================================================
# Crear conexión DuckDB
# ============================================================

def create_connection() -> duckdb.DuckDBPyConnection:
    """
    Crea una base DuckDB en memoria.

    No se genera todavía ningún archivo .db.
    DuckDB funciona solamente como motor SQL para consultar
    nuestro Parquet.
    """

    return duckdb.connect(
        database=":memory:"
    )


# ============================================================
# Crear vista analítica
# ============================================================

def create_baseline_view(
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
) -> None:
    """
    Crea una vista SQL llamada baseline.

    La vista apunta directamente al archivo Parquet,
    por lo que no duplicamos los datos.
    """

    parquet_path_sql = parquet_path.as_posix()

    con.execute(
        f"""
        CREATE OR REPLACE VIEW baseline AS

        SELECT *
        FROM read_parquet('{parquet_path_sql}')
        """
    )


# ============================================================
# Resumen general
# ============================================================

def show_general_summary(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Obtiene estadísticas descriptivas principales
    de la sesión baseline.
    """

    query = """
        SELECT
            COUNT(*) AS records,

            MIN(device_timestamp)
                AS start_time,

            MAX(device_timestamp)
                AS end_time,

            ROUND(
                AVG(temperature_c),
                2
            ) AS avg_temperature_c,

            ROUND(
                MIN(temperature_c),
                2
            ) AS min_temperature_c,

            ROUND(
                MAX(temperature_c),
                2
            ) AS max_temperature_c,

            ROUND(
                AVG(humidity_pct),
                2
            ) AS avg_humidity_pct,

            ROUND(
                AVG(light_raw),
                2
            ) AS avg_light_raw,

            ROUND(
                AVG(wifi_rssi),
                2
            ) AS avg_wifi_rssi,

            ROUND(
                AVG(ingestion_latency_ms),
                2
            ) AS avg_latency_ms

        FROM baseline
    """

    print()
    print("==========================")
    print("BASELINE GENERAL SUMMARY")
    print("==========================")

    # DuckDB devuelve directamente una relación tabular.
    # .show() evita depender de pandas para visualizarla.
    con.sql(query).show()


# ============================================================
# Estado de iluminación
# ============================================================

def show_light_analysis(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Analiza cuántas mediciones fueron clasificadas
    como bright o dark.
    """

    query = """
        SELECT
            light_state,

            COUNT(*) AS records,

            ROUND(
                COUNT(*) * 100.0
                / SUM(COUNT(*)) OVER (),
                2
            ) AS percentage,

            ROUND(
                AVG(light_raw),
                2
            ) AS avg_light_raw

        FROM baseline

        GROUP BY light_state

        ORDER BY records DESC
    """

    print()
    print("==========================")
    print("LIGHT CONDITIONS")
    print("==========================")

    con.sql(query).show()


# ============================================================
# Activaciones del LED
# ============================================================

def show_led_analysis(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Comprueba cuántas veces la lógica edge
    activó automáticamente el LED.
    """

    query = """
        SELECT
            led_active,

            COUNT(*) AS records,

            ROUND(
                COUNT(*) * 100.0
                / SUM(COUNT(*)) OVER (),
                2
            ) AS percentage

        FROM baseline

        GROUP BY led_active

        ORDER BY led_active
    """

    print()
    print("==========================")
    print("LED ACTIVATION")
    print("==========================")

    con.sql(query).show()


# ============================================================
# Calidad de conectividad
# ============================================================

def show_connectivity_analysis(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Analiza RSSI y latencia del pipeline IoT.
    """

    query = """
        SELECT

            ROUND(
                AVG(wifi_rssi),
                2
            ) AS avg_rssi,

            MIN(wifi_rssi)
                AS worst_rssi,

            MAX(wifi_rssi)
                AS best_rssi,

            ROUND(
                AVG(ingestion_latency_ms),
                2
            ) AS avg_latency_ms,

            MEDIAN(
                ingestion_latency_ms
            ) AS median_latency_ms,

            QUANTILE_CONT(
                ingestion_latency_ms,
                0.95
            ) AS p95_latency_ms,

            MAX(
                ingestion_latency_ms
            ) AS max_latency_ms

        FROM baseline
    """

    print()
    print("==========================")
    print("CONNECTIVITY")
    print("==========================")

    con.sql(query).show()


# ============================================================
# Variabilidad de sensores
# ============================================================

def show_sensor_variability(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Calcula desviación estándar de los sensores.

    Esto empieza a establecer qué tan variable es el
    comportamiento normal del entorno.
    """

    query = """
        SELECT

            ROUND(
                STDDEV_SAMP(temperature_c),
                3
            ) AS temperature_std,

            ROUND(
                STDDEV_SAMP(humidity_pct),
                3
            ) AS humidity_std,

            ROUND(
                STDDEV_SAMP(light_raw),
                3
            ) AS light_std,

            ROUND(
                STDDEV_SAMP(wifi_rssi),
                3
            ) AS rssi_std

        FROM baseline
    """

    print()
    print("==========================")
    print("BASELINE VARIABILITY")
    print("==========================")

    con.sql(query).show()


# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Ejecuta el análisis SQL completo del baseline.
    """

    print()
    print("OfficePulse SQL Analytics")
    print("=========================")

    # 1. Encontrar y comprobar el baseline más reciente.
    parquet_path = find_latest_baseline()
    validate_dataset(parquet_path)

    print(f"Baseline seleccionado: {parquet_path}")

    # 2. Inicializar DuckDB.
    con = create_connection()

    try:

        # 3. Exponer el Parquet como una vista SQL.
        create_baseline_view(con, parquet_path)

        # 4. Ejecutar análisis descriptivo.
        show_general_summary(con)

        # 5. Analizar iluminación.
        show_light_analysis(con)

        # 6. Analizar decisiones del LED.
        show_led_analysis(con)

        # 7. Analizar red y latencia AWS.
        show_connectivity_analysis(con)

        # 8. Analizar variabilidad normal de sensores.
        show_sensor_variability(con)

    finally:

        # 9. Cerrar explícitamente la conexión.
        con.close()


# Ejecutar únicamente al llamar este archivo directamente.
if __name__ == "__main__":
    main()
