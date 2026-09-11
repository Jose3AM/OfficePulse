# ============================================================
# Librerías
# ============================================================

# Path se utiliza para administrar rutas de archivos.
from pathlib import Path
import sys

# NumPy se utilizará para operaciones estadísticas
# y para preparar matrices para scikit-learn.
import numpy as np

# Polars se utiliza para leer y manipular nuestro
# dataset de features.
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Salida portable para consolas Windows sin soporte Unicode completo.
pl.Config.set_tbl_formatting("ASCII_FULL")

# IsolationForest implementa detección de anomalías
# no supervisada mediante árboles aleatorios.
from sklearn.ensemble import IsolationForest

# RobustScaler normaliza utilizando mediana e IQR,
# por lo que es menos sensible a outliers que StandardScaler.
from sklearn.preprocessing import RobustScaler


# ============================================================
# Configuración
# ============================================================

# Dataset generado por feature_engineering.py.
FEATURE_PATH = Path(
    "data/features/officepulse_features.parquet"
)

# Carpeta donde guardaremos los resultados del modelado.
OUTPUT_DIR = Path(
    "data/model_output"
)

# Archivo final con scores y predicciones.
RESULTS_PATH = (
    OUTPUT_DIR
    / "anomaly_results.parquet"
)


# ============================================================
# Features utilizadas por los detectores
# ============================================================

# Usaremos únicamente comportamiento temporal.

# No usamos:
# - led_active_ratio
# - dark_ratio
#
# porque contienen decisiones tomadas por la lógica edge
# y podrían introducir leakage conceptual.
FEATURE_COLUMNS = [
    "temperature_change_magnitude",
    "humidity_change_magnitude",
    "light_change_magnitude",
]


# ============================================================
# Configuración del detector estadístico
# ============================================================

# Un robust z-score mayor a aproximadamente 3.5 suele
# considerarse una desviación fuerte del comportamiento normal.
ROBUST_Z_THRESHOLD = 3.5


# ============================================================
# Configuración del Isolation Forest
# ============================================================

# Utilizaremos el percentil 95 de los scores obtenidos
# en el conjunto normal de entrenamiento como threshold.
#
# Esto significa que permitimos que una pequeña parte del
# comportamiento normal extremo defina el límite.
IF_TRAIN_THRESHOLD_QUANTILE = 0.95


# ============================================================
# Cargar datos
# ============================================================

def load_features() -> pl.DataFrame:
    """
    Carga el dataset preparado por feature_engineering.py.
    """

    # Comprobar primero que exista.
    if not FEATURE_PATH.exists():

        raise FileNotFoundError(
            f"No existe el dataset: {FEATURE_PATH}"
        )

    # Leer Parquet utilizando Polars.
    df = pl.read_parquet(
        FEATURE_PATH
    )

    print()
    print("=== FEATURE DATASET ===")
    print(f"Registros : {df.height}")
    print(f"Columnas  : {df.width}")

    return df


# ============================================================
# Separar conjuntos
# ============================================================

def split_datasets(
    df: pl.DataFrame,
) -> tuple[
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
]:
    """
    Divide los datos según la función que definimos
    durante Feature Engineering.

    baseline_01:
        entrenamiento normal.

    baseline_02:
        validación con otro periodo normal.

    light_transition:
        prueba controlada.

    temperature_transition:
        prueba controlada.
    """

    train = df.filter(
        pl.col("evaluation_group")
        == "train_normal"
    )

    validation = df.filter(
        pl.col("evaluation_group")
        == "validation_normal"
    )

    test_light = df.filter(
        pl.col("evaluation_group")
        == "test_light"
    )

    test_temperature = df.filter(
        pl.col("evaluation_group")
        == "test_temperature"
    )

    print()
    print("=== DATA SPLIT ===")

    print(
        f"Train normal        : {train.height}"
    )

    print(
        f"Validation normal   : {validation.height}"
    )

    print(
        f"Test light          : {test_light.height}"
    )

    print(
        f"Test temperature    : {test_temperature.height}"
    )

    return (
        train,
        validation,
        test_light,
        test_temperature,
    )


# ============================================================
# Estadísticas robustas del comportamiento normal
# ============================================================

def calculate_robust_statistics(
    train: pl.DataFrame,
) -> dict:
    """
    Calcula MEDIANA y MAD utilizando exclusivamente
    baseline_01.

    MAD = Median Absolute Deviation.

    A diferencia de media + desviación estándar,
    estas métricas son mucho menos sensibles a valores extremos.
    """

    statistics = {}

    for column in FEATURE_COLUMNS:

        # Convertir la columna a NumPy.
        values = (
            train[column]
            .to_numpy()
            .astype(float)
        )

        # Mediana del comportamiento normal.
        median = np.median(values)

        # Median Absolute Deviation.
        mad = np.median(
            np.abs(
                values - median
            )
        )

        # ----------------------------------------------------
        # Algunos sensores producen valores discretos.
        #
        # En esos casos MAD puede ser exactamente 0.
        #
        # Para evitar división entre cero utilizamos
        # desviación estándar como fallback.
        # ----------------------------------------------------

        if mad == 0:

            fallback_scale = np.std(
                values,
                ddof=1,
            )

            # Si incluso la desviación es cero,
            # usamos un epsilon pequeño.
            scale = max(
                fallback_scale,
                1e-9,
            )

            method = "std_fallback"

        else:

            # 1.4826 convierte aproximadamente MAD
            # a una escala comparable con sigma normal.
            scale = 1.4826 * mad

            method = "MAD"

        statistics[column] = {
            "median": median,
            "scale": scale,
            "method": method,
        }

    return statistics


# ============================================================
# Robust statistical detector
# ============================================================

def apply_statistical_detector(
    df: pl.DataFrame,
    statistics: dict,
) -> pl.DataFrame:
    """
    Calcula robust z-score para cada feature.

    Luego utiliza el máximo z-score absoluto de cada fila
    como score estadístico general.

    Si cualquiera de las variables se aleja demasiado
    del baseline, el registro puede considerarse anómalo.
    """

    result = df.clone()

    z_columns = []

    # --------------------------------------------------------
    # Calcular z-score independiente para cada feature.
    # --------------------------------------------------------

    for column in FEATURE_COLUMNS:

        median = statistics[column]["median"]
        scale = statistics[column]["scale"]

        z_column = (
            f"{column}_robust_z"
        )

        result = result.with_columns(

            (
                (
                    pl.col(column)
                    - median
                )
                / scale
            )

            .abs()

            .alias(z_column)
        )

        z_columns.append(z_column)

    # --------------------------------------------------------
    # Score general:
    #
    # tomar la desviación más extrema entre todas
    # las features.
    # --------------------------------------------------------

    result = result.with_columns(

        pl.max_horizontal(
            z_columns
        )

        .alias(
            "statistical_anomaly_score"
        )
    )

    # --------------------------------------------------------
    # Clasificación final del detector estadístico.
    # --------------------------------------------------------

    result = result.with_columns(

        (
            pl.col(
                "statistical_anomaly_score"
            )
            > ROBUST_Z_THRESHOLD
        )

        .alias(
            "statistical_anomaly"
        )
    )

    return result


# ============================================================
# Preparar datos para Isolation Forest
# ============================================================

def dataframe_to_matrix(
    df: pl.DataFrame,
) -> np.ndarray:
    """
    Convierte las columnas seleccionadas de Polars
    a una matriz NumPy compatible con scikit-learn.
    """

    return (
        df
        .select(FEATURE_COLUMNS)
        .to_numpy()
        .astype(float)
    )


# ============================================================
# Entrenar Isolation Forest
# ============================================================

def train_isolation_forest(
    train: pl.DataFrame,
) -> tuple[
    RobustScaler,
    IsolationForest,
    float,
]:
    """
    Entrena Isolation Forest utilizando exclusivamente
    baseline_01.

    También calcula un threshold a partir de los scores
    observados en los datos normales de entrenamiento.
    """

    # Convertir train a matriz numérica.
    x_train = dataframe_to_matrix(
        train
    )

    # --------------------------------------------------------
    # Normalización robusta.
    #
    # Aunque Isolation Forest no requiere estrictamente
    # normalización, utilizarla mantiene las escalas
    # de las features más comparables.
    # --------------------------------------------------------

    scaler = RobustScaler()

    x_train_scaled = (
        scaler.fit_transform(
            x_train
        )
    )

    # --------------------------------------------------------
    # Crear modelo.
    #
    # n_estimators=300:
    # cantidad razonable de árboles para este proyecto.
    #
    # contamination="auto":
    # no le decimos al algoritmo qué porcentaje debe ser
    # anomalía; calcularemos nuestro propio threshold.
    #
    # random_state:
    # garantiza resultados reproducibles.
    # --------------------------------------------------------

    model = IsolationForest(
        n_estimators=300,
        contamination="auto",
        random_state=42,
        n_jobs=-1,
    )

    model.fit(
        x_train_scaled
    )

    # --------------------------------------------------------
    # decision_function:
    #
    # valores altos  -> más normales
    # valores bajos  -> más anómalos
    #
    # Lo invertimos para que:
    #
    # valor alto -> mayor anomalía
    # --------------------------------------------------------

    train_scores = (
        -model.decision_function(
            x_train_scaled
        )
    )

    # --------------------------------------------------------
    # Threshold definido únicamente a partir del baseline.
    # --------------------------------------------------------

    threshold = np.quantile(
        train_scores,
        IF_TRAIN_THRESHOLD_QUANTILE,
    )

    return (
        scaler,
        model,
        float(threshold),
    )


# ============================================================
# Aplicar Isolation Forest
# ============================================================

def apply_isolation_forest(
    df: pl.DataFrame,
    scaler: RobustScaler,
    model: IsolationForest,
    threshold: float,
) -> pl.DataFrame:
    """
    Calcula anomaly score y predicción para un DataFrame.
    """

    # Convertir Polars -> NumPy.
    x = dataframe_to_matrix(
        df
    )

    # Aplicar exactamente la transformación aprendida
    # con baseline_01.
    x_scaled = scaler.transform(
        x
    )

    # Obtener anomaly score.
    anomaly_scores = (
        -model.decision_function(
            x_scaled
        )
    )

    # Comparar contra threshold aprendido del baseline.
    anomalies = (
        anomaly_scores
        > threshold
    )

    # Añadir resultados al DataFrame.
    return df.with_columns(

        pl.Series(
            name="if_anomaly_score",
            values=anomaly_scores,
        ),

        pl.Series(
            name="if_anomaly",
            values=anomalies,
        ),
    )


# ============================================================
# Aplicar ambos detectores a todo el dataset
# ============================================================

def run_detection(
    df: pl.DataFrame,
    statistics: dict,
    scaler: RobustScaler,
    model: IsolationForest,
    if_threshold: float,
) -> pl.DataFrame:
    """
    Ejecuta los dos métodos de detección:

    1. Statistical detector.
    2. Isolation Forest.
    """

    # Detector estadístico.
    result = apply_statistical_detector(
        df,
        statistics,
    )

    # Isolation Forest.
    result = apply_isolation_forest(
        result,
        scaler,
        model,
        if_threshold,
    )

    # --------------------------------------------------------
    # Crear consenso entre detectores.
    #
    # True únicamente cuando AMBOS consideran
    # que el registro es anómalo.
    # --------------------------------------------------------

    result = result.with_columns(

        (
            pl.col("statistical_anomaly")
            &
            pl.col("if_anomaly")
        )

        .alias(
            "consensus_anomaly"
        )
    )

    return result


# ============================================================
# Evaluación por grupo
# ============================================================

def evaluate_results(
    df: pl.DataFrame,
) -> None:
    """
    Compara qué porcentaje de observaciones fue señalado
    por cada detector.

    IMPORTANTE:

    Las pruebas controladas no tienen labels precisos
    minuto por minuto.

    Por eso hablamos de "flag rate" y no de accuracy,
    precision o recall.
    """

    summary = (
        df

        .group_by(
            "evaluation_group"
        )

        .agg(
            [
                # Número total de registros.
                pl.len()
                .alias("records"),

                # Número de anomalías estadísticas.
                pl.col(
                    "statistical_anomaly"
                )
                .sum()
                .alias(
                    "statistical_flags"
                ),

                # Porcentaje de anomalías estadísticas.
                (
                    pl.col(
                        "statistical_anomaly"
                    )
                    .mean()
                    * 100
                )
                .round(2)
                .alias(
                    "statistical_flag_pct"
                ),

                # Número de flags Isolation Forest.
                pl.col(
                    "if_anomaly"
                )
                .sum()
                .alias(
                    "if_flags"
                ),

                # Porcentaje Isolation Forest.
                (
                    pl.col(
                        "if_anomaly"
                    )
                    .mean()
                    * 100
                )
                .round(2)
                .alias(
                    "if_flag_pct"
                ),

                # Número de anomalías detectadas
                # simultáneamente por ambos.
                pl.col(
                    "consensus_anomaly"
                )
                .sum()
                .alias(
                    "consensus_flags"
                ),

                # Porcentaje de consenso.
                (
                    pl.col(
                        "consensus_anomaly"
                    )
                    .mean()
                    * 100
                )
                .round(2)
                .alias(
                    "consensus_flag_pct"
                ),
            ]
        )

        .sort(
            "evaluation_group"
        )
    )

    print()
    print("=======================================")
    print("ANOMALY DETECTION RESULTS")
    print("=======================================")

    with pl.Config(
        tbl_cols=-1,
        tbl_width_chars=180,
        tbl_formatting="ASCII_FULL",
    ):
        print(summary)


# ============================================================
# Mostrar configuración estadística
# ============================================================

def show_statistical_baseline(
    statistics: dict,
) -> None:
    """
    Muestra cuáles fueron los parámetros aprendidos
    del comportamiento normal.
    """

    print()
    print("=======================================")
    print("ROBUST STATISTICAL BASELINE")
    print("=======================================")

    for column in FEATURE_COLUMNS:

        stats = statistics[column]

        print()
        print(column)

        print(
            f"  Median : "
            f"{stats['median']:.4f}"
        )

        print(
            f"  Scale  : "
            f"{stats['scale']:.4f}"
        )

        print(
            f"  Method : "
            f"{stats['method']}"
        )


# ============================================================
# Guardar resultados
# ============================================================

def save_results(
    df: pl.DataFrame,
) -> None:
    """
    Guarda todos los scores y flags de anomalía
    en formato Parquet.
    """

    # Crear carpeta si todavía no existe.
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Guardar Parquet comprimido.
    df.write_parquet(
        RESULTS_PATH,
        compression="zstd",
    )

    print()
    print("=======================================")
    print("MODEL OUTPUT")
    print("=======================================")

    print(
        f"Archivo : {RESULTS_PATH}"
    )

    print(
        f"Filas   : {df.height}"
    )


# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Pipeline completo de anomaly detection.

    Features
       ↓
    baseline_01
       ↓
    ┌───────────────────────────┐
    │ Robust statistics         │
    │ Isolation Forest          │
    └───────────────────────────┘
       ↓
    baseline_02
    light test
    temperature test
       ↓
    comparación
    """

    print()
    print("OfficePulse Anomaly Detection")
    print("=============================")

    # --------------------------------------------------------
    # 1. Leer dataset de features.
    # --------------------------------------------------------

    df = load_features()

    # --------------------------------------------------------
    # 2. Separar grupos.
    # --------------------------------------------------------

    (
        train,
        validation,
        test_light,
        test_temperature,
    ) = split_datasets(df)

    # --------------------------------------------------------
    # 3. Aprender comportamiento estadístico normal
    #    exclusivamente de baseline_01.
    # --------------------------------------------------------

    statistics = (
        calculate_robust_statistics(
            train
        )
    )

    show_statistical_baseline(
        statistics
    )

    # --------------------------------------------------------
    # 4. Entrenar Isolation Forest únicamente con
    #    baseline_01.
    # --------------------------------------------------------

    (
        scaler,
        model,
        if_threshold,
    ) = train_isolation_forest(
        train
    )

    print()
    print("=======================================")
    print("ISOLATION FOREST")
    print("=======================================")

    print(
        f"Training records : {train.height}"
    )

    print(
        f"Trees            : {model.n_estimators}"
    )

    print(
        f"Score threshold  : {if_threshold:.6f}"
    )

    # --------------------------------------------------------
    # 5. Ejecutar ambos detectores sobre TODOS los grupos.
    # --------------------------------------------------------

    results = run_detection(
        df,
        statistics,
        scaler,
        model,
        if_threshold,
    )

    # --------------------------------------------------------
    # 6. Comparar comportamiento.
    # --------------------------------------------------------

    evaluate_results(
        results
    )

    # --------------------------------------------------------
    # 7. Guardar scores y predicciones.
    # --------------------------------------------------------

    save_results(
        results
    )

    print()
    print("Anomaly detection completed.")


# ============================================================
# Punto de entrada
# ============================================================

if __name__ == "__main__":
    main()
