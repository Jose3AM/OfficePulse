# ============================================================
# Librerías
# ============================================================

# Path facilita trabajar con la ruta del archivo Parquet.
from pathlib import Path
import sys

# Polars se utilizará para agrupar e inspeccionar
# las diferentes ejecuciones físicas del ESP32.
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Salida portable para consolas Windows sin soporte Unicode completo.
pl.Config.set_tbl_formatting("ASCII_FULL")


# ============================================================
# Configuración
# ============================================================

# Dataset consolidado creado por nuestro pipeline.
DATASET_PATH = Path(
    "data/processed/officepulse_all.parquet"
)


# ============================================================
# Carga del dataset
# ============================================================

def load_data() -> pl.DataFrame:
    """
    Carga el dataset procesado y verifica primero
    que el archivo exista.
    """

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No existe el dataset: {DATASET_PATH}"
        )

    return pl.read_parquet(
        DATASET_PATH
    )


# ============================================================
# Analizar ejecuciones del dispositivo
# ============================================================

def analyze_session_runs(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Agrupa los datos por session_id + boot_id.

    session_id identifica el experimento lógico.

    boot_id identifica una ejecución física específica
    del ESP32 entre reinicios.

    Esto permite detectar si reutilizamos accidentalmente
    un mismo session_id en diferentes momentos.
    """

    runs = (
        df

        # Cada combinación session_id + boot_id
        # representa una ejecución independiente.
        .group_by(
            [
                "session_id",
                "boot_id",
            ]
        )

        .agg(
            [
                # Número de registros de la ejecución.
                pl.len()
                .alias("records"),

                # Primer timestamp registrado.
                pl.col("device_timestamp_ms")
                .min()
                .alias("start_ms"),

                # Último timestamp registrado.
                pl.col("device_timestamp_ms")
                .max()
                .alias("end_ms"),

                # Temperatura mínima observada.
                pl.col("temperature_c")
                .min()
                .alias("temp_min"),

                # Temperatura máxima observada.
                pl.col("temperature_c")
                .max()
                .alias("temp_max"),
            ]
        )

        # Calcular duración aproximada de cada ejecución.
        .with_columns(
            (
                (
                    pl.col("end_ms")
                    - pl.col("start_ms")
                )
                / 60000
            )
            .round(2)
            .alias("duration_minutes")
        )

        # Convertir timestamps UNIX para que podamos
        # interpretarlos fácilmente.
        .with_columns(
            [
                pl.from_epoch(
                    pl.col("start_ms"),
                    time_unit="ms",
                )
                .dt.replace_time_zone("UTC")
                .alias("start_time"),

                pl.from_epoch(
                    pl.col("end_ms"),
                    time_unit="ms",
                )
                .dt.replace_time_zone("UTC")
                .alias("end_time"),
            ]
        )

        # Ordenar las ejecuciones cronológicamente
        # dentro de cada experimento.
        .sort(
            [
                "session_id",
                "start_ms",
            ]
        )
    )

    return runs


# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Identifica todas las ejecuciones reales que existen
    dentro de nuestros session_id.
    """

    print()
    print("OfficePulse Session Run Analysis")
    print("===============================")

    # Cargar dataset.
    df = load_data()

    # Analizar combinaciones session + boot.
    runs = analyze_session_runs(df)

    # Mostrar todas las columnas para evitar que Polars
    # trunque información importante.
    with pl.Config(
        tbl_cols=-1,
        tbl_width_chars=220,
        tbl_formatting="ASCII_FULL",
    ):
        print(runs)


# Ejecutar únicamente al llamar este archivo directamente.
if __name__ == "__main__":
    main()
