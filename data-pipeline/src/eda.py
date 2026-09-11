# ============================================================
# Librerías
# ============================================================

# Path se utiliza para localizar nuestro dataset Parquet.
from pathlib import Path
import sys

# Polars se encarga de leer y transformar los datos.
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Salida portable para consolas Windows sin soporte Unicode completo.
pl.Config.set_tbl_formatting("ASCII_FULL")

# Matplotlib genera las visualizaciones.
import matplotlib.pyplot as plt


# ============================================================
# Configuración
# ============================================================

# Dataset consolidado creado por build_dataset.py.
DATASET_PATH = Path(
    "data/processed/officepulse_all.parquet"
)

# Carpeta donde guardaremos las gráficas del análisis.
OUTPUT_DIR = Path(
    "data/eda"
)


# ============================================================
# Cargar dataset
# ============================================================

def load_data() -> pl.DataFrame:
    """
    Carga el dataset Parquet consolidado.

    El Parquet ya pasó por nuestra capa de transformación
    y Data Quality, por lo que esta etapa se enfoca en análisis.
    """

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No existe el dataset: {DATASET_PATH}"
        )

    df = pl.read_parquet(
        DATASET_PATH
    )

    return df


# ============================================================
# Resumen estadístico completo
# ============================================================

def show_session_statistics(
    df: pl.DataFrame,
) -> None:
    """
    Compara las principales variables de cada sesión.

    Incluimos mínimo, máximo, promedio y desviación estándar
    para entender no solo el nivel de cada variable,
    sino también su variabilidad.
    """

    summary = (
        df
        .group_by("session_id")
        .agg(
            # ----------------------------------------------
            # Número de observaciones.
            # ----------------------------------------------
            pl.len()
            .alias("records"),

            # ----------------------------------------------
            # Temperatura.
            # ----------------------------------------------
            pl.col("temperature_c")
            .min()
            .round(2)
            .alias("temp_min"),

            pl.col("temperature_c")
            .mean()
            .round(2)
            .alias("temp_mean"),

            pl.col("temperature_c")
            .max()
            .round(2)
            .alias("temp_max"),

            pl.col("temperature_c")
            .std()
            .round(3)
            .alias("temp_std"),

            # ----------------------------------------------
            # Humedad.
            # ----------------------------------------------
            pl.col("humidity_pct")
            .min()
            .round(2)
            .alias("humidity_min"),

            pl.col("humidity_pct")
            .mean()
            .round(2)
            .alias("humidity_mean"),

            pl.col("humidity_pct")
            .max()
            .round(2)
            .alias("humidity_max"),

            pl.col("humidity_pct")
            .std()
            .round(3)
            .alias("humidity_std"),

            # ----------------------------------------------
            # Iluminación.
            # ----------------------------------------------
            pl.col("light_raw")
            .min()
            .alias("light_min"),

            pl.col("light_raw")
            .mean()
            .round(2)
            .alias("light_mean"),

            pl.col("light_raw")
            .max()
            .alias("light_max"),

            pl.col("light_raw")
            .std()
            .round(2)
            .alias("light_std"),

            # ----------------------------------------------
            # Conectividad.
            # ----------------------------------------------
            pl.col("wifi_rssi")
            .mean()
            .round(2)
            .alias("rssi_mean"),

            pl.col("ingestion_latency_ms")
            .mean()
            .round(2)
            .alias("latency_mean"),
        )
        .sort("session_id")
    )

    print()
    print("====================================")
    print("SESSION STATISTICS")
    print("====================================")

    # Configuramos temporalmente Polars para mostrar
    # todas las columnas y no truncar la tabla.
    with pl.Config(
        tbl_cols=-1,
        tbl_width_chars=220,
        tbl_formatting="ASCII_FULL",
    ):
        print(summary)


# ============================================================
# Crear tiempo relativo
# ============================================================

def add_relative_time(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Genera 'elapsed_minutes'.

    Cada sesión comienza en minuto 0 independientemente
    de la fecha real en que fue ejecutada.

    Esto facilita comparar experimentos con diferentes
    horas de inicio.
    """

    return (
        df
        .with_columns(
            (
                (
                    pl.col("device_timestamp_ms")
                    - pl.col("device_timestamp_ms")
                    .min()
                    .over("session_id")
                )
                / 60000
            )
            .alias("elapsed_minutes")
        )
    )


# ============================================================
# Gráfica de temperatura
# ============================================================

def plot_temperature(
    df: pl.DataFrame,
) -> None:
    """
    Grafica la evolución de temperatura de todas las sesiones.
    """

    plt.figure(
        figsize=(11, 6)
    )

    # Crear una línea independiente para cada experimento.
    for session in df["session_id"].unique().to_list():

        session_df = (
            df
            .filter(
                pl.col("session_id") == session
            )
            .sort("device_timestamp_ms")
        )

        plt.plot(
            session_df["elapsed_minutes"].to_list(),
            session_df["temperature_c"].to_list(),
            label=session,
        )

    plt.xlabel("Minutos desde inicio de sesión")
    plt.ylabel("Temperatura (°C)")
    plt.title("OfficePulse - Temperatura por sesión")
    plt.legend()

    plt.tight_layout()

    output_path = (
        OUTPUT_DIR / "temperature_sessions.png"
    )

    plt.savefig(
        output_path,
        dpi=150,
    )

    plt.close()


# ============================================================
# Gráfica de humedad
# ============================================================

def plot_humidity(
    df: pl.DataFrame,
) -> None:
    """
    Grafica la evolución temporal de humedad.
    """

    plt.figure(
        figsize=(11, 6)
    )

    for session in df["session_id"].unique().to_list():

        session_df = (
            df
            .filter(
                pl.col("session_id") == session
            )
            .sort("device_timestamp_ms")
        )

        plt.plot(
            session_df["elapsed_minutes"].to_list(),
            session_df["humidity_pct"].to_list(),
            label=session,
        )

    plt.xlabel("Minutos desde inicio de sesión")
    plt.ylabel("Humedad (%)")
    plt.title("OfficePulse - Humedad por sesión")
    plt.legend()

    plt.tight_layout()

    plt.savefig(
        OUTPUT_DIR / "humidity_sessions.png",
        dpi=150,
    )

    plt.close()


# ============================================================
# Gráfica de iluminación
# ============================================================

def plot_light(
    df: pl.DataFrame,
) -> None:
    """
    Grafica los valores del LDR.

    También dibuja el threshold utilizado por la lógica
    edge para diferenciar dark y bright.
    """

    plt.figure(
        figsize=(11, 6)
    )

    for session in df["session_id"].unique().to_list():

        session_df = (
            df
            .filter(
                pl.col("session_id") == session
            )
            .sort("device_timestamp_ms")
        )

        plt.plot(
            session_df["elapsed_minutes"].to_list(),
            session_df["light_raw"].to_list(),
            label=session,
        )

    # Nuestro firmware utiliza actualmente threshold = 200.
    plt.axhline(
        y=200,
        linestyle="--",
        label="Dark threshold",
    )

    plt.xlabel("Minutos desde inicio de sesión")
    plt.ylabel("LDR raw value")
    plt.title("OfficePulse - Iluminación por sesión")
    plt.legend()

    plt.tight_layout()

    plt.savefig(
        OUTPUT_DIR / "light_sessions.png",
        dpi=150,
    )

    plt.close()


# ============================================================
# Gráfica específica de transición de luz
# ============================================================

def plot_light_transition(
    df: pl.DataFrame,
) -> None:
    """
    Analiza específicamente light_transition_01.

    Esto nos permite visualizar tanto el sensor como
    las decisiones tomadas por el ESP32.
    """

    session_df = (
        df
        .filter(
            pl.col("session_id")
            == "light_transition_01"
        )
        .sort("device_timestamp_ms")
    )

    if session_df.is_empty():
        return

    plt.figure(
        figsize=(11, 6)
    )

    # Señal cruda del sensor LDR.
    plt.plot(
        session_df["elapsed_minutes"].to_list(),
        session_df["light_raw"].to_list(),
        label="LDR",
    )

    # Threshold utilizado por el firmware.
    plt.axhline(
        y=200,
        linestyle="--",
        label="Dark threshold",
    )

    plt.xlabel("Minutos desde inicio de sesión")
    plt.ylabel("LDR raw value")
    plt.title(
        "OfficePulse - Light Transition Experiment"
    )

    plt.legend()

    plt.tight_layout()

    plt.savefig(
        OUTPUT_DIR / "light_transition.png",
        dpi=150,
    )

    plt.close()


# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Ejecuta nuestro primer EDA reproducible.

    Parquet
        ↓
    Polars
        ↓
    estadísticas
        ↓
    series temporales
        ↓
    imágenes para análisis / presentación
    """

    print()
    print("OfficePulse Exploratory Data Analysis")
    print("=====================================")

    # Crear carpeta para resultados visuales.
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 1. Leer datos procesados.
    df = load_data()

    # 2. Añadir tiempo relativo por sesión.
    df = add_relative_time(df)

    # 3. Mostrar estadísticas.
    show_session_statistics(df)

    # 4. Generar las gráficas.
    plot_temperature(df)
    plot_humidity(df)
    plot_light(df)
    plot_light_transition(df)

    print()
    print("EDA completado.")
    print(
        f"Gráficas guardadas en: "
        f"{OUTPUT_DIR.resolve()}"
    )


# Ejecutar solamente cuando llamamos este script directamente.
if __name__ == "__main__":
    main()
