# ============================================================
# Librerías
# ============================================================

# Path permite trabajar con rutas de archivos de forma
# independiente del sistema operativo.
from pathlib import Path
import sys

# Polars será utilizado para:
# - leer Parquet;
# - resamplear las series;
# - generar features temporales;
# - guardar el dataset final.
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Salida portable para consolas Windows sin soporte Unicode completo.
pl.Config.set_tbl_formatting("ASCII_FULL")


# ============================================================
# Configuración
# ============================================================

# Dataset CURATED generado por build_dataset.py.
INPUT_PATH = Path(
    "data/processed/officepulse_all.parquet"
)

# baseline_01 permanece reservado para entrenamiento. Cualquier
# otro baseline_N se usa como validación normal independiente.
BASELINE_SESSION_PATTERN = r"^baseline_[0-9]+$"

# Carpeta donde almacenaremos datasets orientados a ML.
FEATURE_DIR = Path(
    "data/features"
)

# Dataset después de normalizar todas las sesiones
# a una frecuencia común de un minuto.
RESAMPLED_PATH = (
    FEATURE_DIR
    / "officepulse_resampled_1min.parquet"
)

# Dataset definitivo con features temporales.
FEATURE_PATH = (
    FEATURE_DIR
    / "officepulse_features.parquet"
)


# ============================================================
# Cargar dataset
# ============================================================

def load_dataset() -> pl.DataFrame:
    """
    Lee el dataset CURATED.

    En este punto los datos ya pasaron por:
    - validación del esquema;
    - Data Quality;
    - selección del boot principal;
    - quarantine.
    """

    # Comprobar que el archivo exista.
    if not INPUT_PATH.exists():

        raise FileNotFoundError(
            f"No existe el dataset: {INPUT_PATH}"
        )

    # Leer directamente el archivo Parquet.
    df = pl.read_parquet(
        INPUT_PATH
    )

    print()
    print("=== INPUT DATASET ===")
    print(f"Registros : {df.height}")
    print(f"Columnas  : {df.width}")

    return df


# ============================================================
# Normalización de frecuencia
# ============================================================

def resample_to_one_minute(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Convierte todas las sesiones a una frecuencia común
    de un minuto.

    ¿Por qué?

    baseline_01 y los demás baseline_N:
        aproximadamente 1 observación / minuto.

    pruebas controladas:
        aproximadamente 1 observación / 10 segundos.

    Comparar directamente ambas frecuencias podría introducir
    sesgo en nuestro modelo.

    Por eso agregamos las observaciones dentro de ventanas
    temporales de 1 minuto.
    """

    # --------------------------------------------------------
    # Primero ordenar correctamente cada serie temporal.
    # --------------------------------------------------------

    df = df.sort(
        [
            "session_id",
            "device_timestamp",
        ]
    )

    # --------------------------------------------------------
    # Crear ventanas dinámicas de 1 minuto.
    #
    # Polars agrupa automáticamente las observaciones según
    # device_timestamp dentro de cada session_id.
    # --------------------------------------------------------

    resampled = (
        df

        .group_by_dynamic(
            index_column="device_timestamp",

            # Una observación agregada por minuto.
            every="1m",

            # Cada experimento se procesa independientemente.
            group_by="session_id",

            # La ventana incluye el límite izquierdo.
            closed="left",

            # El timestamp resultante representa
            # el inicio de la ventana.
            label="left",
        )

        .agg(
            [
                # ------------------------------------------------
                # Número de observaciones originales que
                # terminaron dentro de cada minuto.
                #
                # En baseline normalmente será ~1.
                # En experimentos normalmente será ~6.
                # ------------------------------------------------

                pl.len()
                .alias("samples_in_minute"),

                # ------------------------------------------------
                # Temperatura media durante ese minuto.
                # ------------------------------------------------

                pl.col("temperature_c")
                .mean()
                .alias("temperature_c"),

                # ------------------------------------------------
                # Humedad media.
                # ------------------------------------------------

                pl.col("humidity_pct")
                .mean()
                .alias("humidity_pct"),

                # ------------------------------------------------
                # Luz media.
                #
                # La señal del LDR cambia rápidamente durante
                # las transiciones, por lo que el promedio del
                # minuto resume el estado general.
                # ------------------------------------------------

                pl.col("light_raw")
                .mean()
                .alias("light_raw"),

                # ------------------------------------------------
                # RSSI medio.
                # ------------------------------------------------

                pl.col("wifi_rssi")
                .mean()
                .alias("wifi_rssi"),

                # ------------------------------------------------
                # Latencia media ESP32 -> AWS.
                # ------------------------------------------------

                pl.col("ingestion_latency_ms")
                .mean()
                .alias("ingestion_latency_ms"),

                # ------------------------------------------------
                # Porcentaje del minuto durante el cual
                # el LED estuvo activado.
                #
                # Convertimos bool -> float:
                #
                # False = 0
                # True  = 1
                #
                # Ejemplo:
                # 6 mediciones, LED ON en 3:
                #
                # led_active_ratio = 0.5
                # ------------------------------------------------

                pl.col("led_active")
                .cast(pl.Float64)
                .mean()
                .alias("led_active_ratio"),

                # ------------------------------------------------
                # Porcentaje de observaciones clasificadas
                # como oscuridad.
                # ------------------------------------------------

                (
                    pl.col("light_state") == "dark"
                )
                .cast(pl.Float64)
                .mean()
                .alias("dark_ratio"),
            ]
        )

        # ----------------------------------------------------
        # Ordenar nuevamente después del group_by_dynamic.
        # ----------------------------------------------------

        .sort(
            [
                "session_id",
                "device_timestamp",
            ]
        )
    )

    return resampled


# ============================================================
# Añadir metadata del experimento
# ============================================================

def add_evaluation_groups(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Añade una columna que indica para qué utilizaremos
    cada sesión durante el modelado.

    IMPORTANTE:
    Esta columna NO es una etiqueta supervisada para entrenar.

    Solamente nos permite separar correctamente los conjuntos.
    """

    return df.with_columns(

        pl.when(
            pl.col("session_id") == "baseline_01"
        )
        .then(
            pl.lit("train_normal")
        )

        .when(
            pl.col("session_id")
            .str.contains(BASELINE_SESSION_PATTERN)
        )
        .then(
            pl.lit("validation_normal")
        )

        .when(
            pl.col("session_id")
            == "light_transition_01"
        )
        .then(
            pl.lit("test_light")
        )

        .when(
            pl.col("session_id")
            == "temperature_transition_01"
        )
        .then(
            pl.lit("test_temperature")
        )

        .otherwise(
            pl.lit("unknown")
        )

        .alias("evaluation_group")
    )


# ============================================================
# Feature Engineering temporal
# ============================================================

def create_temporal_features(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Crea features basadas en CAMBIOS temporales.

    Este punto es muy importante.

    No queremos depender únicamente de valores absolutos como:

        temperature_c = 34

    porque 34 °C podría simplemente representar una
    temperatura ambiente diferente.

    Nos interesan también patrones como:

        cambio rápido de temperatura
        cambio brusco de iluminación
        cambio rápido de humedad
    """

    # --------------------------------------------------------
    # PRIMERA ETAPA:
    # calcular diferencias entre observaciones consecutivas.
    # --------------------------------------------------------

    df = df.with_columns(

        # Tiempo transcurrido desde la observación anterior.
        #
        # Después del resampling debería ser normalmente
        # aproximadamente 1 minuto.
        (
            pl.col("device_timestamp")
            .diff()
            .over("session_id")
            .dt.total_seconds()
            / 60.0
        )
        .alias("elapsed_minutes"),

        # Cambio absoluto de temperatura.
        pl.col("temperature_c")
        .diff()
        .over("session_id")
        .alias("temperature_delta"),

        # Cambio absoluto de humedad.
        pl.col("humidity_pct")
        .diff()
        .over("session_id")
        .alias("humidity_delta"),

        # Cambio absoluto de iluminación.
        pl.col("light_raw")
        .diff()
        .over("session_id")
        .alias("light_delta"),
    )

    # --------------------------------------------------------
    # SEGUNDA ETAPA:
    # convertir los cambios a tasas por minuto.
    #
    # Esto hace las features más robustas si existiera
    # algún hueco temporal.
    # --------------------------------------------------------

    df = df.with_columns(

        # Cambio de temperatura por minuto.
        (
            pl.col("temperature_delta")
            / pl.col("elapsed_minutes")
        )
        .alias("temperature_rate_per_min"),

        # Cambio de humedad por minuto.
        (
            pl.col("humidity_delta")
            / pl.col("elapsed_minutes")
        )
        .alias("humidity_rate_per_min"),

        # Cambio de iluminación por minuto.
        (
            pl.col("light_delta")
            / pl.col("elapsed_minutes")
        )
        .alias("light_rate_per_min"),
    )

    # --------------------------------------------------------
    # TERCERA ETAPA:
    # magnitudes absolutas.
    #
    # Para anomaly detection muchas veces nos interesa
    # la magnitud del cambio independientemente de si fue
    # positivo o negativo.
    #
    # Ejemplo:
    #
    # -150 de luz
    # +150 de luz
    #
    # ambos representan transiciones fuertes.
    # --------------------------------------------------------

    df = df.with_columns(

        pl.col("temperature_rate_per_min")
        .abs()
        .alias("temperature_change_magnitude"),

        pl.col("humidity_rate_per_min")
        .abs()
        .alias("humidity_change_magnitude"),

        pl.col("light_rate_per_min")
        .abs()
        .alias("light_change_magnitude"),
    )

    return df


# ============================================================
# Validar dataset de features
# ============================================================

def validate_features(
    df: pl.DataFrame,
) -> None:
    """
    Ejecuta controles simples antes de utilizar las features
    para anomaly detection.
    """

    print()
    print("=======================================")
    print("FEATURE DATA QUALITY")
    print("=======================================")

    print(
        f"Registros resampleados : {df.height}"
    )

    # --------------------------------------------------------
    # Contar observaciones por grupo de evaluación.
    # --------------------------------------------------------

    group_counts = (
        df

        .group_by(
            "evaluation_group"
        )

        .agg(
            pl.len()
            .alias("records")
        )

        .sort(
            "evaluation_group"
        )
    )

    print()
    print("Records by evaluation group:")
    print(group_counts)

    # --------------------------------------------------------
    # Comprobar intervalos temporales.
    # --------------------------------------------------------

    interval_summary = (
        df

        .filter(
            pl.col("elapsed_minutes")
            .is_not_null()
        )

        .group_by(
            "session_id"
        )

        .agg(
            [
                pl.col("elapsed_minutes")
                .min()
                .alias("min_interval"),

                pl.col("elapsed_minutes")
                .mean()
                .round(3)
                .alias("mean_interval"),

                pl.col("elapsed_minutes")
                .max()
                .alias("max_interval"),
            ]
        )

        .sort(
            "session_id"
        )
    )

    print()
    print("Interval validation:")
    print(interval_summary)


# ============================================================
# Guardar datasets
# ============================================================

def save_datasets(
    resampled_df: pl.DataFrame,
    feature_df: pl.DataFrame,
) -> None:
    """
    Guarda dos datasets:

    1. Resampled:
       datos normalizados a 1 minuto.

    2. Features:
       dataset preparado para anomaly detection.
    """

    # Crear carpeta si todavía no existe.
    FEATURE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # Guardar dataset resampleado.
    # --------------------------------------------------------

    resampled_df.write_parquet(
        RESAMPLED_PATH,
        compression="zstd",
    )

    # --------------------------------------------------------
    # Las primeras observaciones de cada sesión tienen null
    # en delta/rate porque no existe una observación anterior.
    #
    # Para modelado las eliminamos.
    # --------------------------------------------------------

    model_df = feature_df.filter(

        pl.col("elapsed_minutes")
        .is_not_null()

        &

        pl.col("temperature_rate_per_min")
        .is_not_null()

        &

        pl.col("humidity_rate_per_min")
        .is_not_null()

        &

        pl.col("light_rate_per_min")
        .is_not_null()
    )

    # Guardar dataset definitivo.
    model_df.write_parquet(
        FEATURE_PATH,
        compression="zstd",
    )

    print()
    print("=======================================")
    print("FEATURE DATASETS CREATED")
    print("=======================================")

    print(
        f"Resampled dataset : {RESAMPLED_PATH}"
    )

    print(
        f"Rows              : {resampled_df.height}"
    )

    print()

    print(
        f"Feature dataset   : {FEATURE_PATH}"
    )

    print(
        f"Rows              : {model_df.height}"
    )


# ============================================================
# Mostrar estadísticas de las nuevas features
# ============================================================

def show_feature_summary(
    df: pl.DataFrame,
) -> None:
    """
    Permite comprobar si los experimentos realmente generan
    cambios mayores que los baselines.
    """

    summary = (

        df

        # Ignorar primera fila de cada sesión,
        # porque los deltas son null.
        .filter(
            pl.col("elapsed_minutes")
            .is_not_null()
        )

        .group_by(
            "session_id"
        )

        .agg(
            [
                # Máximo cambio térmico por minuto.
                pl.col(
                    "temperature_change_magnitude"
                )
                .max()
                .round(3)
                .alias("max_temp_change"),

                # Cambio térmico medio.
                pl.col(
                    "temperature_change_magnitude"
                )
                .mean()
                .round(3)
                .alias("avg_temp_change"),

                # Máximo cambio de humedad.
                pl.col(
                    "humidity_change_magnitude"
                )
                .max()
                .round(3)
                .alias("max_humidity_change"),

                # Máximo cambio de iluminación.
                pl.col(
                    "light_change_magnitude"
                )
                .max()
                .round(2)
                .alias("max_light_change"),

                # Cambio medio de iluminación.
                pl.col(
                    "light_change_magnitude"
                )
                .mean()
                .round(2)
                .alias("avg_light_change"),
            ]
        )

        .sort(
            "session_id"
        )
    )

    print()
    print("=======================================")
    print("TEMPORAL FEATURE SUMMARY")
    print("=======================================")

    with pl.Config(
        tbl_cols=-1,
        tbl_width_chars=180,
        tbl_formatting="ASCII_FULL",
    ):
        print(summary)


# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Ejecuta todo el Feature Engineering.

    CURATED Parquet
          ↓
    1-minute resampling
          ↓
    evaluation groups
          ↓
    temporal features
          ↓
    feature validation
          ↓
    ML-ready Parquet
    """

    print()
    print("OfficePulse Feature Engineering")
    print("===============================")

    # --------------------------------------------------------
    # 1. Leer dataset CURATED.
    # --------------------------------------------------------

    df = load_dataset()

    # --------------------------------------------------------
    # 2. Normalizar todas las sesiones a 1 minuto.
    # --------------------------------------------------------

    resampled_df = (
        resample_to_one_minute(df)
    )

    print()
    print(
        f"Registros después del resampling: "
        f"{resampled_df.height}"
    )

    # --------------------------------------------------------
    # 3. Añadir metadata de evaluación.
    # --------------------------------------------------------

    resampled_df = (
        add_evaluation_groups(
            resampled_df
        )
    )

    # --------------------------------------------------------
    # 4. Crear features temporales.
    # --------------------------------------------------------

    feature_df = (
        create_temporal_features(
            resampled_df
        )
    )

    # --------------------------------------------------------
    # 5. Validar las nuevas features.
    # --------------------------------------------------------

    validate_features(
        feature_df
    )

    # --------------------------------------------------------
    # 6. Mostrar diferencias entre experimentos.
    # --------------------------------------------------------

    show_feature_summary(
        feature_df
    )

    # --------------------------------------------------------
    # 7. Guardar resultados.
    # --------------------------------------------------------

    save_datasets(
        resampled_df,
        feature_df,
    )

    print()
    print("Feature engineering completed.")


# ============================================================
# Punto de entrada
# ============================================================

if __name__ == "__main__":
    main()
