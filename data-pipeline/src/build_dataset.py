# ============================================================
# Librerías
# ============================================================

# json permite leer los archivos JSON individuales
# descargados desde Amazon S3.
import json
import sys

# Path facilita trabajar con rutas de archivos en Windows,
# Linux o macOS sin construir strings manualmente.
from pathlib import Path

# Polars será nuestro motor principal para transformar,
# validar y preparar los datos.
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Evita errores de codificacion al imprimir tablas en consolas
# de Windows que no soportan los bordes Unicode de Polars.
pl.Config.set_tbl_formatting("ASCII_FULL")


# ============================================================
# Configuración general
# ============================================================

# Carpeta que contiene los JSON originales descargados de S3.
RAW_DIR = Path("data/raw")

# Carpeta donde guardaremos los datasets procesados
# y listos para análisis.
PROCESSED_DIR = Path("data/processed")

# Carpeta donde guardaremos registros técnicamente válidos,
# pero que no deben formar parte del dataset analítico.
#
# Importante:
# NO se eliminan de RAW.
# Se conservan aquí para mantener trazabilidad.
QUARANTINE_DIR = Path("data/quarantine")

# Solo queremos trabajar con el esquema definitivo
# utilizado por firmware 0.3.0.
FIRMWARE_VERSION = "0.3.0"

# Las sesiones baseline se descubren automáticamente mediante
# una convención estricta como baseline_03 o baseline_10.
BASELINE_SESSION_PATTERN = r"^baseline_[0-9]+$"

# Las sesiones no-baseline continúan siendo explícitas para
# evitar incorporar por accidente capturas experimentales.
FIXED_VALID_SESSIONS = [
    "light_transition_01",
    "temperature_transition_01",
]


# ============================================================
# Columnas obligatorias
# ============================================================

# Todas estas columnas deberían estar presentes en los mensajes
# generados por OfficePulse firmware 0.3.0.
REQUIRED_COLUMNS = [
    "device_id",
    "session_id",
    "boot_id",
    "sequence_number",
    "device_timestamp_ms",
    "uptime_ms",
    "dht_valid",
    "temperature_c",
    "humidity_pct",
    "light_raw",
    "light_state",
    "led_active",
    "dark_threshold",
    "wifi_rssi",
    "firmware_version",
    "aws_ingest_ts",
    "mqtt_topic",
]


# ============================================================
# Carga de archivos RAW
# ============================================================

def load_raw_records() -> pl.DataFrame:
    """
    Lee todos los archivos JSON almacenados en data/raw.

    Si un archivo está corrupto:
    - no se detiene todo el pipeline;
    - se registra como inválido;
    - se continúa procesando el resto.
    """

    records = []
    invalid_files = []

    # --------------------------------------------------------
    # Recorrer todos los archivos JSON de la capa RAW.
    # --------------------------------------------------------

    for file_path in RAW_DIR.glob("*.json"):

        try:

            # Abrir el archivo y convertir el JSON
            # a un diccionario de Python.
            with file_path.open(
                "r",
                encoding="utf-8",
            ) as file:

                record = json.load(file)

            # Guardamos también el nombre del archivo original.
            #
            # Esto proporciona trazabilidad:
            # si posteriormente encontramos un dato problemático,
            # podremos saber exactamente de qué JSON provino.
            record["_source_file"] = file_path.name

            records.append(record)

        except (json.JSONDecodeError, OSError) as error:

            # Guardar información del archivo problemático.
            invalid_files.append(
                {
                    "file": file_path.name,
                    "error": str(error),
                }
            )

    # --------------------------------------------------------
    # Reporte de lectura RAW.
    # --------------------------------------------------------

    print()
    print("=== RAW FILE REPORT ===")
    print(f"JSON válidos   : {len(records)}")
    print(f"JSON inválidos : {len(invalid_files)}")

    # Si no existen registros válidos, no podemos continuar.
    if not records:
        raise RuntimeError(
            "No se encontraron registros JSON válidos."
        )

    # --------------------------------------------------------
    # Convertir los registros en un DataFrame Polars.
    #
    # infer_schema_length=None hace que Polars inspeccione
    # todos los registros antes de inferir los tipos.
    # --------------------------------------------------------

    return pl.DataFrame(
        records,
        infer_schema_length=None,
    )


# ============================================================
# Validación del esquema
# ============================================================

def validate_schema(df: pl.DataFrame) -> None:
    """
    Comprueba que el dataset contenga todas las columnas
    obligatorias definidas para firmware 0.3.0.
    """

    # Buscar columnas requeridas que no estén presentes.
    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    # Si falta cualquier columna obligatoria,
    # detener el pipeline.
    if missing_columns:
        raise ValueError(
            "Faltan columnas obligatorias: "
            f"{missing_columns}"
        )

    print("Schema validation : OK")


# ============================================================
# Filtrado y transformación
# ============================================================

def transform_data(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Prepara los datos para análisis.

    Operaciones:
    1. Filtra firmware 0.3.0.
    2. Conserva únicamente las sesiones válidas.
    3. Convierte timestamps.
    4. Calcula latencia de ingestión.
    5. Ordena los registros.
    6. Calcula diferencias de sequence_number.
    """

    df = (
        df

        # ----------------------------------------------------
        # 1. Evitar mezclar datos de firmware antiguo.
        # ----------------------------------------------------

        .filter(
            pl.col("firmware_version")
            == FIRMWARE_VERSION
        )

        # ----------------------------------------------------
        # 2. Conservar únicamente nuestras sesiones
        # experimentales.
        # ----------------------------------------------------

        .filter(
            pl.col("session_id")
            .str.contains(BASELINE_SESSION_PATTERN)

            |

            pl.col("session_id")
            .is_in(FIXED_VALID_SESSIONS)
        )

        # ----------------------------------------------------
        # 3. Crear columnas derivadas.
        # ----------------------------------------------------

        .with_columns(

            # Timestamp del ESP32:
            # convertir epoch en milisegundos a DateTime UTC.
            pl.from_epoch(
                pl.col("device_timestamp_ms"),
                time_unit="ms",
            )
            .dt.replace_time_zone("UTC")
            .alias("device_timestamp"),

            # Timestamp de AWS:
            # convertir epoch en milisegundos a DateTime UTC.
            pl.from_epoch(
                pl.col("aws_ingest_ts"),
                time_unit="ms",
            )
            .dt.replace_time_zone("UTC")
            .alias("aws_ingest_timestamp"),

            # Latencia aproximada:
            #
            # momento AWS recibió mensaje
            # -
            # momento ESP32 generó mensaje
            (
                pl.col("aws_ingest_ts")
                - pl.col("device_timestamp_ms")
            )
            .alias("ingestion_latency_ms"),
        )

        # ----------------------------------------------------
        # 4. Ordenar por sesión, boot y sequence number.
        # ----------------------------------------------------

        .sort(
            [
                "session_id",
                "boot_id",
                "sequence_number",
            ]
        )

        # ----------------------------------------------------
        # 5. Calcular diferencia entre números de secuencia.
        #
        # Ejemplo:
        #
        # 10 -> 11 = 1   normal
        # 11 -> 12 = 1   normal
        # 12 -> 15 = 3   faltaron 13 y 14
        #
        # Se calcula por session_id + boot_id para evitar
        # confundir reinicios con pérdida de mensajes.
        # ----------------------------------------------------

        .with_columns(

            pl.col("sequence_number")
            .diff()
            .over(
                [
                    "session_id",
                    "boot_id",
                ]
            )
            .alias("sequence_diff")
        )
    )

    return df


# ============================================================
# Selección de la ejecución principal
# ============================================================

def select_primary_runs(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Identifica cuál boot_id representa la ejecución principal
    de cada experimento.

    Una misma session_id puede tener varios boot_id debido a:

    - reinicio del ESP32;
    - carga de firmware;
    - pruebas accidentales;
    - ejecución posterior con el mismo session_id.

    Para estos experimentos controlados utilizaremos como
    ejecución principal aquella que tenga más registros.

    Devuelve:
        curated_df:
            datos que utilizaremos para analytics.

        quarantine_df:
            registros válidos que pertenecen a ejecuciones
            secundarias.
    """

    # --------------------------------------------------------
    # Contar registros por session_id + boot_id.
    #
    # Cada combinación representa una ejecución física
    # independiente del ESP32.
    # --------------------------------------------------------

    run_summary = (
        df

        .group_by(
            [
                "session_id",
                "boot_id",
            ]
        )

        .agg(
            [
                # Número de observaciones en esa ejecución.
                pl.len()
                .alias("run_records"),

                # Timestamp inicial.
                pl.col("device_timestamp_ms")
                .min()
                .alias("run_start_ms"),

                # Timestamp final.
                pl.col("device_timestamp_ms")
                .max()
                .alias("run_end_ms"),
            ]
        )

        # ----------------------------------------------------
        # Ordenar:
        #
        # 1. session_id
        # 2. mayor número de registros primero
        #
        # Así el primer boot de cada sesión será el principal.
        # ----------------------------------------------------

        .sort(
            [
                "session_id",
                "run_records",
            ],
            descending=[
                False,
                True,
            ],
        )
    )

    # --------------------------------------------------------
    # Seleccionar solamente el boot principal de cada sesión.
    # --------------------------------------------------------

    primary_runs = (
        run_summary

        # Como run_summary está ordenado por cantidad
        # de registros descendente, nos quedamos
        # con el primer boot de cada session_id.
        .unique(
            subset=["session_id"],
            keep="first",
            maintain_order=True,
        )

        # Solo necesitamos session_id y boot_id.
        .select(
            [
                "session_id",
                "boot_id",
            ]
        )

        # Renombrar para poder compararlo con el boot_id
        # original de cada observación.
        .rename(
            {
                "boot_id": "primary_boot_id"
            }
        )
    )

    # --------------------------------------------------------
    # Añadir a cada observación cuál es el boot principal
    # de su sesión.
    # --------------------------------------------------------

    labeled_df = (
        df

        .join(
            primary_runs,
            on="session_id",
            how="left",
        )

        # Crear bandera:
        #
        # True  = pertenece al experimento principal
        # False = ejecución secundaria
        .with_columns(
            (
                pl.col("boot_id")
                == pl.col("primary_boot_id")
            )
            .alias("is_primary_run")
        )
    )

    # --------------------------------------------------------
    # CURATED DATASET
    #
    # Solo conservar ejecuciones principales.
    # --------------------------------------------------------

    curated_df = (
        labeled_df

        .filter(
            pl.col("is_primary_run")
        )

        # Estas columnas eran auxiliares únicamente
        # para realizar la selección.
        .drop(
            [
                "primary_boot_id",
                "is_primary_run",
            ]
        )
    )

    # --------------------------------------------------------
    # QUARANTINE DATASET
    #
    # Conservamos las observaciones excluidas para auditoría
    # y trazabilidad.
    # --------------------------------------------------------

    quarantine_df = (
        labeled_df

        .filter(
            ~pl.col("is_primary_run")
        )

        # Añadimos explícitamente la razón de exclusión.
        .with_columns(

            pl.lit(
                "non_primary_boot_for_session"
            )
            .alias("exclusion_reason")
        )
    )

    return curated_df, quarantine_df


# ============================================================
# Reporte de calidad por sesión
# ============================================================

def quality_report_for_session(
    df: pl.DataFrame,
    session_id: str,
) -> None:
    """
    Ejecuta controles de Data Quality sobre una sola sesión.
    """

    # --------------------------------------------------------
    # Seleccionar únicamente la sesión que queremos analizar.
    # --------------------------------------------------------

    session_df = df.filter(
        pl.col("session_id") == session_id
    )

    # Si la sesión no tiene registros, informar y continuar.
    if session_df.is_empty():

        print()

        print(
            f"WARNING: no existen datos para {session_id}"
        )

        return

    total_records = session_df.height

    # --------------------------------------------------------
    # Duplicados.
    #
    # Esta combinación debería identificar una observación
    # de forma única.
    # --------------------------------------------------------

    duplicate_count = (
        session_df

        .select(

            pl.struct(
                [
                    "device_id",
                    "session_id",
                    "boot_id",
                    "sequence_number",
                ]
            )

            .is_duplicated()

            .sum()

            .alias("duplicates")
        )

        .item()
    )

    # --------------------------------------------------------
    # Lecturas inválidas del sensor DHT11.
    # --------------------------------------------------------

    invalid_dht = (
        session_df

        .filter(
            ~pl.col("dht_valid")
        )

        .height
    )

    # --------------------------------------------------------
    # Humedad fuera del rango físico 0-100%.
    #
    # Ignoramos null porque puede representar correctamente
    # una lectura DHT inválida.
    # --------------------------------------------------------

    invalid_humidity = (
        session_df

        .filter(

            pl.col("humidity_pct")
            .is_not_null()

            &

            (
                ~pl.col("humidity_pct")
                .is_between(
                    0,
                    100,
                    closed="both",
                )
            )
        )

        .height
    )

    # --------------------------------------------------------
    # Validar valores del ADC.
    #
    # El ESP32 está configurado a 12 bits:
    #
    # mínimo = 0
    # máximo = 4095
    # --------------------------------------------------------

    invalid_light = (
        session_df

        .filter(

            ~pl.col("light_raw")
            .is_between(
                0,
                4095,
                closed="both",
            )
        )

        .height
    )

    # --------------------------------------------------------
    # Detectar saltos en sequence_number.
    #
    # sequence_diff > 1 significa que hubo un salto.
    # --------------------------------------------------------

    sequence_gaps = (
        session_df

        .filter(
            pl.col("sequence_diff") > 1
        )

        .height
    )

    # --------------------------------------------------------
    # Estimar cuántos mensajes faltaron.
    #
    # Ejemplo:
    #
    # sequence:
    # 10 -> 13
    #
    # difference = 3
    #
    # mensajes faltantes:
    # 11 y 12
    #
    # missing = 3 - 1 = 2
    # --------------------------------------------------------

    missing_messages_result = (
        session_df

        .filter(
            pl.col("sequence_diff") > 1
        )

        .select(

            (
                pl.col("sequence_diff") - 1
            )

            .sum()

            .alias("missing")
        )

        .item()
    )

    # Si no existen gaps Polars puede devolver None.
    missing_messages = (
        missing_messages_result
        if missing_messages_result is not None
        else 0
    )

    # --------------------------------------------------------
    # Número de boots dentro de la sesión curada.
    #
    # Después de nuestra selección debería ser 1.
    # --------------------------------------------------------

    unique_boots = (
        session_df

        .select(

            pl.col("boot_id")
            .n_unique()
        )

        .item()
    )

    detected_reboots = max(
        unique_boots - 1,
        0,
    )

    # --------------------------------------------------------
    # Validar clasificación de iluminación.
    #
    # Regla firmware:
    #
    # light_raw < dark_threshold
    #     -> dark
    #
    # light_raw >= dark_threshold
    #     -> bright
    # --------------------------------------------------------

    expected_light_state = (

        pl.when(
            pl.col("light_raw")
            < pl.col("dark_threshold")
        )

        .then(
            pl.lit("dark")
        )

        .otherwise(
            pl.lit("bright")
        )
    )

    light_state_errors = (
        session_df

        .filter(
            pl.col("light_state")
            != expected_light_state
        )

        .height
    )

    # --------------------------------------------------------
    # Validar lógica del LED.
    #
    # dark   -> LED ON
    # bright -> LED OFF
    # --------------------------------------------------------

    expected_led = (
        pl.col("light_raw")
        < pl.col("dark_threshold")
    )

    led_logic_errors = (
        session_df

        .filter(
            pl.col("led_active")
            != expected_led
        )

        .height
    )

    # --------------------------------------------------------
    # Estadísticas de latencia ESP32 -> AWS.
    # --------------------------------------------------------

    latency = (
        session_df

        .select(
            [
                # Media.
                pl.col("ingestion_latency_ms")
                .mean()
                .alias("mean"),

                # Mediana.
                pl.col("ingestion_latency_ms")
                .median()
                .alias("median"),

                # Percentil 95.
                pl.col("ingestion_latency_ms")
                .quantile(0.95)
                .alias("p95"),

                # Máximo.
                pl.col("ingestion_latency_ms")
                .max()
                .alias("max"),
            ]
        )

        .row(
            0,
            named=True,
        )
    )

    # --------------------------------------------------------
    # RSSI medio de la conexión Wi-Fi.
    # --------------------------------------------------------

    avg_rssi = (
        session_df

        .select(

            pl.col("wifi_rssi")
            .mean()
        )

        .item()
    )

    # --------------------------------------------------------
    # Mostrar reporte final.
    # --------------------------------------------------------

    print()
    print("=======================================")
    print(f"SESSION: {session_id}")
    print("=======================================")

    print(f"Records              : {total_records}")
    print(f"Duplicate records    : {duplicate_count}")
    print(f"Invalid DHT readings : {invalid_dht}")
    print(f"Invalid humidity     : {invalid_humidity}")
    print(f"Invalid light values : {invalid_light}")

    print()
    print("--- DELIVERY QUALITY ---")

    print(f"Sequence gaps        : {sequence_gaps}")
    print(f"Estimated missing    : {missing_messages}")
    print(f"Device boots         : {unique_boots}")
    print(f"Detected reboots     : {detected_reboots}")

    print()
    print("--- EDGE LOGIC ---")

    print(f"Light state errors   : {light_state_errors}")
    print(f"LED logic errors     : {led_logic_errors}")

    print()
    print("--- CLOUD LATENCY ---")

    print(
        f"Mean latency         : "
        f"{latency['mean']:.2f} ms"
    )

    print(
        f"Median latency       : "
        f"{latency['median']:.2f} ms"
    )

    print(
        f"P95 latency          : "
        f"{latency['p95']:.2f} ms"
    )

    print(
        f"Max latency          : "
        f"{latency['max']:.2f} ms"
    )

    print()
    print("--- CONNECTIVITY ---")

    print(
        f"Average WiFi RSSI    : "
        f"{avg_rssi:.2f} dBm"
    )


# ============================================================
# Resumen general de sesiones
# ============================================================

def show_session_summary(
    df: pl.DataFrame,
) -> None:
    """
    Genera una tabla comparativa entre las sesiones curadas.
    """

    summary = (
        df

        .group_by("session_id")

        .agg(

            # ------------------------------------------------
            # Cantidad de registros.
            # ------------------------------------------------

            pl.len()
            .alias("records"),

            # ------------------------------------------------
            # Temperatura.
            # ------------------------------------------------

            pl.col("temperature_c")
            .mean()
            .round(2)
            .alias("avg_temperature_c"),

            pl.col("temperature_c")
            .std()
            .round(3)
            .alias("std_temperature_c"),

            # ------------------------------------------------
            # Humedad.
            # ------------------------------------------------

            pl.col("humidity_pct")
            .mean()
            .round(2)
            .alias("avg_humidity_pct"),

            # ------------------------------------------------
            # Iluminación.
            # ------------------------------------------------

            pl.col("light_raw")
            .mean()
            .round(2)
            .alias("avg_light_raw"),

            pl.col("light_raw")
            .std()
            .round(2)
            .alias("std_light_raw"),

            # ------------------------------------------------
            # Wi-Fi.
            # ------------------------------------------------

            pl.col("wifi_rssi")
            .mean()
            .round(2)
            .alias("avg_wifi_rssi"),

            # ------------------------------------------------
            # AWS latency.
            # ------------------------------------------------

            pl.col("ingestion_latency_ms")
            .mean()
            .round(2)
            .alias("avg_latency_ms"),

            # ------------------------------------------------
            # Registros clasificados como dark.
            # ------------------------------------------------

            (
                pl.col("light_state") == "dark"
            )
            .sum()
            .alias("dark_records"),

            # ------------------------------------------------
            # Activaciones del LED.
            # ------------------------------------------------

            pl.col("led_active")
            .sum()
            .alias("led_activations"),
        )

        .sort("session_id")
    )

    print()
    print("=======================================")
    print("SESSION COMPARISON")
    print("=======================================")

    # Mostrar todas las columnas de la tabla.
    with pl.Config(
        tbl_cols=-1,
        tbl_width_chars=220,
        tbl_formatting="ASCII_FULL",
    ):
        print(summary)


# ============================================================
# Guardar registros en quarantine
# ============================================================

def save_quarantine_data(
    df: pl.DataFrame,
) -> None:
    """
    Guarda registros válidos que fueron excluidos
    del dataset analítico.

    No eliminamos estos datos porque queremos conservar
    trazabilidad completa desde RAW hasta CURATED.
    """

    # --------------------------------------------------------
    # Si no existe ningún registro excluido,
    # simplemente informamos y terminamos.
    # --------------------------------------------------------

    if df.is_empty():

        print()
        print("=== QUARANTINE ===")
        print("Registros excluidos : 0")

        return

    # --------------------------------------------------------
    # Crear carpeta quarantine.
    # --------------------------------------------------------

    QUARANTINE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Archivo de salida.
    output_path = (
        QUARANTINE_DIR
        / "excluded_records.parquet"
    )

    # --------------------------------------------------------
    # Guardar como Parquet comprimido.
    # --------------------------------------------------------

    df.write_parquet(
        output_path,
        compression="zstd",
    )

    # --------------------------------------------------------
    # Mostrar resumen.
    # --------------------------------------------------------

    print()
    print("=== QUARANTINE ===")

    print(
        f"Registros excluidos : {df.height}"
    )

    print(
        f"Archivo             : {output_path}"
    )

    # Mostrar qué registros fueron excluidos.
    quarantine_summary = (
        df

        .group_by(
            [
                "session_id",
                "boot_id",
                "exclusion_reason",
            ]
        )

        .agg(
            pl.len()
            .alias("records")
        )

        .sort("session_id")
    )

    print()
    print("Quarantine summary:")

    print(
        quarantine_summary
    )


# ============================================================
# Guardar datasets procesados
# ============================================================

def save_processed_data(
    df: pl.DataFrame,
) -> None:
    """
    Genera:

    1. officepulse_all.parquet
       Dataset curado completo.

    2. Un Parquet independiente para cada sesión.
    """

    # --------------------------------------------------------
    # Crear carpeta processed.
    # --------------------------------------------------------

    PROCESSED_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # Guardar dataset curado completo.
    # --------------------------------------------------------

    all_output = (
        PROCESSED_DIR
        / "officepulse_all.parquet"
    )

    df.write_parquet(
        all_output,
        compression="zstd",
    )

    print()
    print("=== PROCESSED DATA ===")

    print(
        f"Dataset completo : {all_output}"
    )

    print(
        f"Registros        : {df.height}"
    )

    print(
        f"Columnas         : {df.width}"
    )

    # --------------------------------------------------------
    # Guardar un dataset separado por sesión.
    # --------------------------------------------------------

    session_ids = (
        df["session_id"]
        .unique()
        .sort()
        .to_list()
    )

    for session_id in session_ids:

        # Obtener registros de esa sesión.
        session_df = df.filter(
            pl.col("session_id")
            == session_id
        )

        # No generar archivos vacíos.
        if session_df.is_empty():
            continue

        # Construir nombre automáticamente.
        session_output = (
            PROCESSED_DIR
            / f"{session_id}.parquet"
        )

        # Guardar.
        session_df.write_parquet(
            session_output,
            compression="zstd",
        )

        print(
            f"Dataset sesión   : "
            f"{session_output} "
            f"({session_df.height} filas)"
        )


# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Ejecuta el pipeline completo de OfficePulse.

    Flujo:

    RAW JSON
        ↓
    Schema Validation
        ↓
    Filtering
        ↓
    Transformations
        ↓
    Run Selection
        ↓
       / \
      /   \
     ↓     ↓
 CURATED  QUARANTINE
     ↓
 Data Quality
     ↓
 Parquet
    """

    print()
    print("OfficePulse Data Pipeline")
    print("=========================")

    # --------------------------------------------------------
    # 1. Leer todos los JSON originales.
    # --------------------------------------------------------

    df = load_raw_records()

    print(
        f"Registros RAW totales: {df.height}"
    )

    # --------------------------------------------------------
    # 2. Validar que las columnas obligatorias existan.
    # --------------------------------------------------------

    validate_schema(df)

    # --------------------------------------------------------
    # 3. Filtrar firmware/sesiones y crear columnas derivadas.
    # --------------------------------------------------------

    df = transform_data(df)

    print(
        f"Registros después de transformación: "
        f"{df.height}"
    )

    # Si después de los filtros no queda nada,
    # detener el pipeline.
    if df.is_empty():

        raise RuntimeError(
            "El dataset quedó vacío después "
            "de los filtros."
        )

    # --------------------------------------------------------
    # 4. Separar ejecución principal y registros secundarios.
    # --------------------------------------------------------

    curated_df, quarantine_df = (
        select_primary_runs(df)
    )

    print()
    print("=== RUN SELECTION ===")

    print(
        f"Registros antes de selección : "
        f"{df.height}"
    )

    print(
        f"Registros curated            : "
        f"{curated_df.height}"
    )

    print(
        f"Registros quarantine         : "
        f"{quarantine_df.height}"
    )

    # Validación adicional:
    # si por algún motivo todo quedó en quarantine,
    # detenemos el pipeline.
    if curated_df.is_empty():

        raise RuntimeError(
            "No quedaron registros en la capa curated."
        )

    # --------------------------------------------------------
    # 5. Ejecutar Data Quality solamente sobre CURATED.
    #
    # Esta es una diferencia importante respecto a la versión
    # anterior: los registros secundarios ya no contaminan
    # nuestros indicadores de calidad.
    # --------------------------------------------------------

    session_ids = (
        curated_df["session_id"]
        .unique()
        .sort()
        .to_list()
    )

    for session_id in session_ids:

        quality_report_for_session(
            curated_df,
            session_id,
        )

    # --------------------------------------------------------
    # 6. Comparar las sesiones limpias.
    # --------------------------------------------------------

    show_session_summary(
        curated_df
    )

    # --------------------------------------------------------
    # 7. Guardar los registros excluidos.
    #
    # RAW sigue intacto.
    # Quarantine guarda evidencia de por qué ciertas filas
    # no entraron al dataset analítico.
    # --------------------------------------------------------

    save_quarantine_data(
        quarantine_df
    )

    # --------------------------------------------------------
    # 8. Guardar dataset CURATED en Parquet.
    # --------------------------------------------------------

    save_processed_data(
        curated_df
    )

    # --------------------------------------------------------
    # 9. Mensaje final.
    # --------------------------------------------------------

    print()
    print("=======================================")
    print("PIPELINE COMPLETED")
    print("=======================================")

    print(
        f"RAW records        : {df.height}"
    )

    print(
        f"CURATED records    : {curated_df.height}"
    )

    print(
        f"QUARANTINE records : {quarantine_df.height}"
    )


# ============================================================
# Punto de entrada
# ============================================================

# Este bloque hace que main() se ejecute únicamente cuando
# llamamos directamente:
#
# python src/build_dataset.py
#
# y evita ejecutarlo accidentalmente si este módulo
# es importado desde otro archivo.
if __name__ == "__main__":
    main()
