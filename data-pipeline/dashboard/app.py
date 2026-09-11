# ============================================================
# OFFICEPULSE DASHBOARD
# ============================================================
#
# Dashboard de analítica ambiental:
#
# ESP32
#   ↓
# MQTT / TLS
#   ↓
# AWS IoT Core
#   ↓
# Amazon S3
#   ↓
# Polars / Parquet
#   ↓
# Anomaly Detection
#   ↓
# Streamlit
#
# ============================================================


# ============================================================
# Librerías
# ============================================================

# Path permite manejar rutas de archivos de forma segura.
from pathlib import Path
import re

# Polars se utiliza para leer y transformar los Parquet.
import polars as pl

# Plotly genera las gráficas interactivas.
import plotly.express as px

# Streamlit construye la aplicación web.
import streamlit as st


# ============================================================
# Configuración general
# ============================================================

st.set_page_config(
    page_title="OfficePulse",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# CSS
# ============================================================
#
# IMPORTANTE:
#
# Aquí solamente definimos estilos.
#
# Más abajo, los componentes HTML personalizados se generarán
# como strings de UNA SOLA LÍNEA.
#
# Esto evita el problema que estaba provocando que Streamlit
# mostrara literalmente:
#
# <div class="kpi-label">
# ...
#
# ============================================================

st.markdown(
    """
<style>

    /* --------------------------------------------------------
       Layout principal
       -------------------------------------------------------- */

    .block-container {
        max-width: 1420px;
        padding-top: 2rem;
        padding-bottom: 3rem;
    }


    /* --------------------------------------------------------
       Hero / encabezado
       -------------------------------------------------------- */

    .officepulse-hero {
        padding: 1.75rem 1.9rem;
        margin-bottom: 1.35rem;

        border-radius: 18px;

        border:
            1px solid rgba(34, 197, 94, 0.28);

        background:
            linear-gradient(
                135deg,
                rgba(16, 185, 129, 0.11),
                rgba(6, 182, 212, 0.035)
            );
    }

    .officepulse-title {
        display: block;

        margin: 0;

        font-size: 2.55rem;
        line-height: 1.05;

        font-weight: 760;

        letter-spacing: -0.045em;
    }

    .officepulse-subtitle {
        display: block;

        max-width: 760px;

        margin-top: 0.7rem;

        font-size: 1rem;

        opacity: 0.68;
    }

    .session-badge {
        display: inline-block;

        padding:
            0.34rem
            0.72rem;

        border-radius: 999px;

        border:
            1px solid
            rgba(34, 197, 94, 0.35);

        background:
            rgba(34, 197, 94, 0.09);

        font-size: 0.78rem;

        font-weight: 650;
    }

    .hero-meta {
        display: flex;

        align-items: center;

        flex-wrap: wrap;

        gap: 0.75rem 1rem;

        margin-top: 1rem;
    }


    /* --------------------------------------------------------
       Section labels
       -------------------------------------------------------- */

    .section-eyebrow {
        display: block;

        margin-top: 0.5rem;
        margin-bottom: 0.25rem;

        font-size: 0.70rem;

        font-weight: 750;

        letter-spacing: 0.12em;

        text-transform: uppercase;

        opacity: 0.52;
    }

    .section-description {
        display: block;

        margin-top: -0.35rem;
        margin-bottom: 1.2rem;

        font-size: 0.91rem;

        opacity: 0.63;
    }


    /* --------------------------------------------------------
       KPI cards
       -------------------------------------------------------- */

    .kpi-card {
        min-height: 138px;

        padding: 1.05rem 1.15rem;

        border-radius: 15px;

        border:
            1px solid
            rgba(128, 128, 128, 0.20);

        background:
            rgba(128, 128, 128, 0.045);

        transition:
            border-color 0.15s ease,
            transform 0.15s ease;
    }

    .kpi-card:hover {
        border-color:
            rgba(34, 197, 94, 0.40);

        transform:
            translateY(-1px);
    }

    .kpi-icon {
        display: block;

        margin-bottom: 0.48rem;

        font-size: 1.15rem;
    }

    .kpi-label {
        display: block;

        margin-bottom: 0.28rem;

        font-size: 0.78rem;

        font-weight: 600;

        opacity: 0.62;
    }

    .kpi-value {
        display: block;

        font-size: 1.75rem;

        line-height: 1.15;

        font-weight: 720;

        letter-spacing: -0.025em;
    }

    .kpi-detail {
        display: block;

        margin-top: 0.48rem;

        font-size: 0.76rem;

        opacity: 0.54;
    }


    /* --------------------------------------------------------
       Status indicator
       -------------------------------------------------------- */

    .system-status {
        display: inline-flex;

        align-items: center;

        gap: 0.45rem;

        font-size: 0.77rem;

        opacity: 0.72;
    }

    .status-dot {
        display: inline-block;

        width: 8px;
        height: 8px;

        border-radius: 50%;

        background: #22c55e;
    }


    /* --------------------------------------------------------
       Sidebar
       -------------------------------------------------------- */

    section[data-testid="stSidebar"] {
        border-right:
            1px solid
            rgba(128, 128, 128, 0.15);
    }


    /* --------------------------------------------------------
       Tabs
       -------------------------------------------------------- */

    div[data-baseweb="tab-list"] {
        gap: 1.25rem;
    }


    /* --------------------------------------------------------
       Footer
       -------------------------------------------------------- */

    .officepulse-footer {
        padding-top: 0.8rem;

        text-align: center;

        font-size: 0.77rem;

        line-height: 1.7;

        opacity: 0.45;
    }

    .footer-line {
        display: block;
    }

</style>
""",
    unsafe_allow_html=True,
)


# ============================================================
# Rutas del proyecto
# ============================================================

# app.py está en:
#
# data_pipeline/dashboard/app.py
#
# Por eso parent.parent representa:
#
# data_pipeline/
PROJECT_ROOT = Path(
    __file__
).resolve().parent.parent


# Dataset CURATED.
DATA_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "officepulse_all.parquet"
)


# Resultados de anomaly detection.
ANOMALY_PATH = (
    PROJECT_ROOT
    / "data"
    / "model_output"
    / "anomaly_results.parquet"
)


# ============================================================
# Carga de datos
# ============================================================

@st.cache_data
def load_environmental_data() -> pl.DataFrame:
    """
    Carga el dataset ambiental CURATED.

    st.cache_data evita volver a leer el Parquet cada vez
    que el usuario cambia un filtro.
    """

    if not DATA_PATH.exists():

        raise FileNotFoundError(
            f"No existe el archivo: {DATA_PATH}"
        )

    return pl.read_parquet(
        DATA_PATH
    )


@st.cache_data
def load_anomaly_data() -> pl.DataFrame:
    """
    Carga los resultados generados por anomaly_detection.py.
    """

    if not ANOMALY_PATH.exists():

        raise FileNotFoundError(
            f"No existe el archivo: {ANOMALY_PATH}"
        )

    return pl.read_parquet(
        ANOMALY_PATH
    )


# ============================================================
# Helpers
# ============================================================

SPANISH_TEXT = {
    "Filters": "Filtros",
    "Current view": "Vista actual",
    "Environmental IoT Analytics": "Analítica ambiental IoT",
    "Session": "Sesión",
    "Data Pipeline": "Flujo de datos",
    "Source": "Fuente",
    "ESP32 Environmental Node": "Nodo ambiental ESP32",
    "Cloud": "Nube",
    "Processing": "Procesamiento",
    "Analytics": "Analítica",
    "Environmental monitoring, edge control and anomaly detection powered by AWS IoT.": "Monitoreo ambiental, control en el borde y detección de anomalías con AWS IoT.",
    "Historical dataset available": "Datos históricos disponibles",
    "Overview": "Resumen",
    "Historical Analysis": "Análisis histórico",
    "Anomaly Detection": "Detección de anomalías",
    "Environment": "Ambiente",
    "Environmental Status": "Estado ambiental",
    "Latest observation available for the selected experimental session.": "Última observación disponible para la sesión experimental seleccionada.",
    "Temperature": "Temperatura",
    "Ambient temperature": "Temperatura ambiente",
    "Humidity": "Humedad",
    "Relative humidity": "Humedad relativa",
    "Light Level": "Nivel de luz",
    "Low light detected": "Nivel de luz bajo detectado",
    "Sufficient ambient light": "Luz ambiental suficiente",
    "Device & Cloud": "Dispositivo y nube",
    "System Health": "Estado del sistema",
    "Wi-Fi Signal": "Señal Wi-Fi",
    "Excellent signal": "Señal excelente",
    "Good signal": "Señal buena",
    "Stable signal": "Señal estable",
    "Weak signal": "Señal débil",
    "AWS Ingestion Latency": "Latencia de ingesta en AWS",
    "Lighting Control": "Control de iluminación",
    "Automatic lighting active": "Iluminación automática activa",
    "Automatic lighting inactive": "Iluminación automática inactiva",
    "Telemetry": "Telemetría",
    "Environmental Trends": "Tendencias ambientales",
    "Time": "Hora",
    "Light Level & Edge Threshold": "Nivel de luz y umbral del dispositivo",
    "LDR raw value": "Valor bruto del LDR",
    "Dark threshold": "Umbral de oscuridad",
    "Light Transition": "Transición de luz",
    "Temperature Transition": "Transición de temperatura",
    "Compare environmental conditions and system performance across controlled experimental sessions.": "Compara las condiciones ambientales y el desempeño del sistema entre sesiones experimentales controladas.",
    "Records": "Registros",
    "Avg Temperature (°C)": "Temperatura prom. (°C)",
    "Avg Humidity (%)": "Humedad prom. (%)",
    "Avg Light": "Luz prom.",
    "Avg RSSI (dBm)": "RSSI prom. (dBm)",
    "Avg Latency (ms)": "Latencia prom. (ms)",
    "LED Activations": "Activaciones del LED",
    "Temperature Across Sessions": "Temperatura entre sesiones",
    "Minutes since session start": "Minutos desde el inicio de la sesión",
    "Light Level Across Sessions": "Nivel de luz entre sesiones",
    "Reliability": "Confiabilidad",
    "Data Quality": "Calidad de los datos",
    "Curated Records": "Registros preparados",
    "Analytics-ready observations": "Observaciones listas para análisis",
    "Valid DHT Readings": "Lecturas DHT válidas",
    "valid": "válidas",
    "Edge Logic Errors": "Errores de lógica en el borde",
    "Sessions": "Sesiones",
    "Controlled experiments": "Experimentos controlados",
    "LDR → state → LED consistency": "Consistencia LDR → estado → LED",
    "Machine Learning": "Aprendizaje automático",
    "Compare a robust statistical detector with Isolation Forest over controlled environmental events.": "Compara un detector estadístico robusto con Isolation Forest en eventos ambientales controlados.",
    "Session to analyze": "Sesión para analizar",
    "Statistical Detector": "Detector estadístico",
    "flags": "alertas",
    "of observations": "de las observaciones",
    "detected by both": "detectadas por ambos",
    "Consensus": "Consenso",
    "ON": "ENCENDIDO",
    "OFF": "APAGADO",
    "Isolation Forest Anomaly Score": "Puntuación de anomalías de Isolation Forest",
    "Anomaly score": "Puntuación de anomalía",
    "Flagged": "Marcada",
    "Robust Statistical Anomaly Score": "Puntuación estadística robusta de anomalías",
    "Robust anomaly score": "Puntuación robusta de anomalía",
    "Threshold": "Umbral",
    "Consensus Anomalies": "Anomalías por consenso",
    "Observations flagged simultaneously by the statistical detector and Isolation Forest.": "Observaciones marcadas simultáneamente por el detector estadístico e Isolation Forest.",
    "Timestamp": "Fecha y hora",
    "Light": "Luz",
    "Temp Change": "Cambio de temp.",
    "Light Change": "Cambio de luz",
    "Statistical Score": "Puntuación estadística",
    "IF Score": "Puntuación IF",
    "No consensus anomalies detected for this session.": "No se detectaron anomalías por consenso en esta sesión.",
    "OfficePulse · Environmental IoT Analytics on AWS": "OfficePulse · Analítica ambiental IoT en AWS",
}


def translate(text: str) -> str:
    """Return interface text in the language selected in the sidebar."""

    if st.session_state.get("language", "Español") == "Español":
        return SPANISH_TEXT.get(text, text)

    return text


def to_plot_data(
    df: pl.DataFrame,
    columns: list[str],
) -> dict:
    """
    Convierte únicamente las columnas necesarias
    a listas de Python.

    De esta manera Plotly puede utilizar los datos sin
    introducir pandas como dependencia.
    """

    return {
        column: df[column].to_list()
        for column in columns
    }


def format_session_name(
    session_id: str,
) -> str:
    """
    Convierte los nombres técnicos de las sesiones
    en nombres más amigables para el dashboard.
    """

    spanish_selected = (
        st.session_state.get("language", "Español")
        == "Español"
    )

    baseline_match = re.fullmatch(
        r"baseline_([0-9]+)",
        session_id,
    )

    if baseline_match:
        baseline_number = baseline_match.group(1)

        return (
            f"Referencia {baseline_number}"
            if spanish_selected
            else f"Baseline {baseline_number}"
        )

    session_names = {
        "light_transition_01":
            translate("Light Transition"),

        "temperature_transition_01":
            translate("Temperature Transition"),
    }

    return session_names.get(
        session_id,
        session_id
        .replace("_", " ")
        .title(),
    )


def get_wifi_quality(
    rssi: float,
) -> str:
    """
    Clasifica de manera descriptiva el RSSI.
    """

    if rssi >= -50:
        return translate("Excellent signal")

    if rssi >= -60:
        return translate("Good signal")

    if rssi >= -70:
        return translate("Stable signal")

    return translate("Weak signal")


def get_light_description(
    light_state: str,
) -> str:
    """
    Traduce el estado técnico del LDR a una
    descripción entendible.
    """

    if light_state == "dark":
        return translate("Low light detected")

    return translate("Sufficient ambient light")


def render_kpi(
    column,
    icon: str,
    label: str,
    value: str,
    detail: str,
) -> None:
    """
    Renderiza una tarjeta KPI.

    IMPORTANTE:
    todo el HTML se genera como una sola línea.

    No existen saltos de línea ni indentaciones dentro
    del HTML, evitando que Markdown lo convierta en código.
    """

    html = (
        '<div class="kpi-card">'
        f'<span class="kpi-icon">{icon}</span>'
        f'<span class="kpi-label">{label}</span>'
        f'<span class="kpi-value">{value}</span>'
        f'<span class="kpi-detail">{detail}</span>'
        '</div>'
    )

    column.markdown(
        html,
        unsafe_allow_html=True,
    )


def render_section_label(
    label: str,
) -> None:
    """
    Renderiza el pequeño encabezado superior
    de cada sección.
    """

    html = (
        f'<span class="section-eyebrow">'
        f'{label}'
        f'</span>'
    )

    st.markdown(
        html,
        unsafe_allow_html=True,
    )


def render_description(
    text: str,
) -> None:
    """
    Renderiza la descripción corta debajo del título.
    """

    html = (
        f'<span class="section-description">'
        f'{text}'
        f'</span>'
    )

    st.markdown(
        html,
        unsafe_allow_html=True,
    )


def style_figure(
    fig,
    height: int = 360,
) -> None:
    """
    Aplica un diseño consistente a las gráficas Plotly.
    """

    fig.update_layout(

        height=height,

        paper_bgcolor=
            "rgba(0,0,0,0)",

        plot_bgcolor=
            "rgba(0,0,0,0)",

        margin=dict(
            l=20,
            r=20,
            t=55,
            b=20,
        ),

        hovermode=
            "x unified",

        legend_title_text="",
    )

    # Grid discreto.
    fig.update_xaxes(
        gridcolor=
            "rgba(128,128,128,0.12)",

        zeroline=False,
    )

    fig.update_yaxes(
        gridcolor=
            "rgba(128,128,128,0.12)",

        zeroline=False,
    )

    # Grosor uniforme para líneas.
    fig.update_traces(
        line=dict(
            width=2,
        )
    )


# ============================================================
# Cargar datasets
# ============================================================

try:

    environmental_df = (
        load_environmental_data()
    )

    anomaly_df = (
        load_anomaly_data()
    )

except FileNotFoundError as error:

    st.error(
        str(error)
    )

    st.stop()


# ============================================================
# Sidebar
# ============================================================

st.sidebar.markdown(
    "## 🌿 OfficePulse"
)

st.sidebar.radio(
    "Idioma / Language",
    options=["Español", "English"],
    horizontal=True,
    key="language",
)

st.sidebar.caption(
    translate("Environmental IoT Analytics")
)

st.sidebar.divider()


# ------------------------------------------------------------
# Selector de sesión
# ------------------------------------------------------------

st.sidebar.markdown(
    f"### {translate('Filters')}"
)


sessions = (
    environmental_df[
        "session_id"
    ]
    .unique()
    .sort()
    .to_list()
)


selected_session = (
    st.sidebar.selectbox(

        translate("Session"),

        options=sessions,

        format_func=
            format_session_name,
    )
)


st.sidebar.divider()


# ------------------------------------------------------------
# Arquitectura resumida
# ------------------------------------------------------------

st.sidebar.markdown(
    f"### {translate('Data Pipeline')}"
)


st.sidebar.markdown(
    f"""
**{translate("Source")}**  
{translate("ESP32 Environmental Node")}

**{translate("Cloud")}**  
AWS IoT Core → Amazon S3

**{translate("Processing")}**  
Polars → Parquet

**{translate("Analytics")}**  
DuckDB → ML → Streamlit
"""
)


# ============================================================
# Filtrar sesión seleccionada
# ============================================================

session_df = (
    environmental_df

    .filter(
        pl.col("session_id")
        == selected_session
    )

    .sort(
        "device_timestamp"
    )
)


# ============================================================
# Hero
# ============================================================

# Igual que con los KPI:
#
# todo el HTML se genera en una única línea.
hero_html = (
    '<div class="officepulse-hero">'
    '<span class="officepulse-title">'
    'OfficePulse'
    '</span>'
    '<span class="officepulse-subtitle">'
    f'{translate("Environmental monitoring, edge control and anomaly detection powered by AWS IoT.")}'
    '</span>'
    '<div class="hero-meta">'
    '<span class="session-badge">'
    f'{translate("Current view")} · {format_session_name(selected_session)}'
    '</span>'
    '<span class="system-status">'
    '<span class="status-dot"></span>'
    f'{translate("Historical dataset available")}'
    '</span>'
    '</div>'
    '</div>'
)


st.markdown(
    hero_html,
    unsafe_allow_html=True,
)


# ============================================================
# Tabs
# ============================================================

overview_tab, historical_tab, anomaly_tab = (
    st.tabs(
        [
            translate("Overview"),
            translate("Historical Analysis"),
            translate("Anomaly Detection"),
        ]
    )
)


# ============================================================
# TAB 1 — OVERVIEW
# ============================================================

with overview_tab:

    # ========================================================
    # Último registro
    # ========================================================

    latest = (
        session_df
        .tail(1)
    )


    latest_temperature = (
        latest[
            "temperature_c"
        ][0]
    )


    latest_humidity = (
        latest[
            "humidity_pct"
        ][0]
    )


    latest_light = (
        latest[
            "light_raw"
        ][0]
    )


    latest_light_state = (
        latest[
            "light_state"
        ][0]
    )


    latest_rssi = (
        latest[
            "wifi_rssi"
        ][0]
    )


    latest_led = (
        latest[
            "led_active"
        ][0]
    )


    latest_latency = (
        latest[
            "ingestion_latency_ms"
        ][0]
    )


    # ========================================================
    # Environment
    # ========================================================

    render_section_label(
        translate("Environment")
    )


    st.subheader(
        translate("Environmental Status")
    )


    render_description(
        translate(
            "Latest observation available for the selected experimental session."
        )
    )


    env_col1, env_col2, env_col3 = (
        st.columns(3)
    )


    render_kpi(
        env_col1,
        "🌡️",
        translate("Temperature"),
        f"{latest_temperature:.1f} °C",
        translate("Ambient temperature"),
    )


    render_kpi(
        env_col2,
        "💧",
        translate("Humidity"),
        f"{latest_humidity:.0f} %",
        translate("Relative humidity"),
    )


    render_kpi(
        env_col3,
        "☀️",
        translate("Light Level"),
        f"{latest_light:.0f}",
        get_light_description(
            latest_light_state
        ),
    )


    st.write("")


    # ========================================================
    # System Health
    # ========================================================

    render_section_label(
        translate("Device & Cloud")
    )


    st.subheader(
        translate("System Health")
    )


    sys_col1, sys_col2, sys_col3 = (
        st.columns(3)
    )


    render_kpi(
        sys_col1,
        "📶",
        translate("Wi-Fi Signal"),
        f"{latest_rssi:.0f} dBm",
        get_wifi_quality(
            latest_rssi
        ),
    )


    render_kpi(
        sys_col2,
        "☁️",
        translate("AWS Ingestion Latency"),
        f"{latest_latency:.0f} ms",
        "ESP32 → AWS IoT Core",
    )


    render_kpi(
        sys_col3,
        "💡",
        translate("Lighting Control"),
        (
            translate("ON")
            if latest_led
            else translate("OFF")
        ),
        (
            translate("Automatic lighting active")
            if latest_led
            else
            translate("Automatic lighting inactive")
        ),
    )


    st.divider()


    # ========================================================
    # Environmental Trends
    # ========================================================

    render_section_label(
        translate("Telemetry")
    )


    st.subheader(
        translate("Environmental Trends")
    )


    # --------------------------------------------------------
    # Temperature
    # --------------------------------------------------------

    temperature_data = (
        to_plot_data(
            session_df,
            [
                "device_timestamp",
                "temperature_c",
            ],
        )
    )


    temperature_fig = px.line(

        temperature_data,

        x="device_timestamp",

        y="temperature_c",

        title=translate("Temperature"),

        labels={
            "device_timestamp":
                translate("Time"),

            "temperature_c":
                f'{translate("Temperature")} (°C)',
        },
    )


    style_figure(
        temperature_fig,
        height=340,
    )


    # --------------------------------------------------------
    # Humidity
    # --------------------------------------------------------

    humidity_data = (
        to_plot_data(
            session_df,
            [
                "device_timestamp",
                "humidity_pct",
            ],
        )
    )


    humidity_fig = px.line(

        humidity_data,

        x="device_timestamp",

        y="humidity_pct",

        title=translate("Humidity"),

        labels={
            "device_timestamp":
                translate("Time"),

            "humidity_pct":
                f'{translate("Humidity")} (%)',
        },
    )


    style_figure(
        humidity_fig,
        height=340,
    )


    # Mostrar ambos charts en paralelo.
    trend_col1, trend_col2 = (
        st.columns(2)
    )


    with trend_col1:

        st.plotly_chart(
            temperature_fig,
            width="stretch",
        )


    with trend_col2:

        st.plotly_chart(
            humidity_fig,
            width="stretch",
        )


    # --------------------------------------------------------
    # Light
    # --------------------------------------------------------

    light_data = (
        to_plot_data(
            session_df,
            [
                "device_timestamp",
                "light_raw",
            ],
        )
    )


    light_fig = px.line(

        light_data,

        x="device_timestamp",

        y="light_raw",

        title=
            translate("Light Level & Edge Threshold"),

        labels={
            "device_timestamp":
                translate("Time"),

            "light_raw":
                translate("LDR raw value"),
        },
    )


    # Threshold utilizado por el ESP32.
    light_fig.add_hline(

        y=200,

        line_dash="dash",

        annotation_text=
            translate("Dark threshold"),
    )


    style_figure(
        light_fig,
        height=370,
    )


    st.plotly_chart(
        light_fig,
        width="stretch",
    )


# ============================================================
# TAB 2 — HISTORICAL ANALYSIS
# ============================================================

with historical_tab:

    render_section_label(
        translate("Analytics")
    )


    st.subheader(
        translate("Historical Analysis")
    )


    render_description(
        translate(
            "Compare environmental conditions and system performance "
            "across controlled experimental sessions."
        )
    )


    # ========================================================
    # Resumen por sesión
    # ========================================================

    session_summary = (

        environmental_df

        .group_by(
            "session_id"
        )

        .agg(
            [

                pl.len()
                .alias(
                    translate("Records")
                ),

                pl.col(
                    "temperature_c"
                )
                .mean()
                .round(2)
                .alias(
                    translate("Avg Temperature (°C)")
                ),

                pl.col(
                    "humidity_pct"
                )
                .mean()
                .round(2)
                .alias(
                    translate("Avg Humidity (%)")
                ),

                pl.col(
                    "light_raw"
                )
                .mean()
                .round(2)
                .alias(
                    translate("Avg Light")
                ),

                pl.col(
                    "wifi_rssi"
                )
                .mean()
                .round(2)
                .alias(
                    translate("Avg RSSI (dBm)")
                ),

                pl.col(
                    "ingestion_latency_ms"
                )
                .mean()
                .round(2)
                .alias(
                    translate("Avg Latency (ms)")
                ),

                pl.col(
                    "led_active"
                )
                .sum()
                .alias(
                    translate("LED Activations")
                ),
            ]
        )

        .sort(
            "session_id"
        )

        .rename(
            {
                "session_id":
                    translate("Session")
            }
        )
    )


    st.dataframe(
        session_summary,
        width="stretch",
        hide_index=True,
    )


    st.divider()


    # ========================================================
    # Crear tiempo relativo
    # ========================================================

    historical_df = (

        environmental_df

        .sort(
            [
                "session_id",
                "device_timestamp_ms",
            ]
        )

        .with_columns(

            (
                (
                    pl.col(
                        "device_timestamp_ms"
                    )

                    -

                    pl.col(
                        "device_timestamp_ms"
                    )
                    .min()
                    .over(
                        "session_id"
                    )
                )

                / 60000
            )

            .alias(
                "elapsed_minutes"
            )
        )
    )


    # ========================================================
    # Temperature Across Sessions
    # ========================================================

    historical_temp_data = (

        to_plot_data(
            historical_df,
            [
                "elapsed_minutes",
                "temperature_c",
                "session_id",
            ],
        )
    )


    historical_temp_fig = px.line(

        historical_temp_data,

        x="elapsed_minutes",

        y="temperature_c",

        color="session_id",

        title=
            translate("Temperature Across Sessions"),

        labels={
            "elapsed_minutes":
                translate("Minutes since session start"),

            "temperature_c":
                f'{translate("Temperature")} (°C)',

            "session_id":
                translate("Session"),
        },
    )


    style_figure(
        historical_temp_fig,
        height=420,
    )


    st.plotly_chart(
        historical_temp_fig,
        width="stretch",
    )


    # ========================================================
    # Light Across Sessions
    # ========================================================

    historical_light_data = (

        to_plot_data(
            historical_df,
            [
                "elapsed_minutes",
                "light_raw",
                "session_id",
            ],
        )
    )


    historical_light_fig = px.line(

        historical_light_data,

        x="elapsed_minutes",

        y="light_raw",

        color="session_id",

        title=
            translate("Light Level Across Sessions"),

        labels={
            "elapsed_minutes":
                translate("Minutes since session start"),

            "light_raw":
                translate("LDR raw value"),

            "session_id":
                translate("Session"),
        },
    )


    historical_light_fig.add_hline(

        y=200,

        line_dash="dash",

        annotation_text=
            translate("Dark threshold"),
    )


    style_figure(
        historical_light_fig,
        height=420,
    )


    st.plotly_chart(
        historical_light_fig,
        width="stretch",
    )


    st.divider()


    # ========================================================
    # Data Quality
    # ========================================================

    render_section_label(
        translate("Reliability")
    )


    st.subheader(
        translate("Data Quality")
    )


    # Lecturas DHT válidas.
    valid_dht_count = (

        environmental_df

        .filter(
            pl.col(
                "dht_valid"
            )
        )

        .height
    )


    # Errores entre la regla del LDR
    # y el estado real del LED.
    logic_errors = (

        environmental_df

        .filter(

            pl.col(
                "led_active"
            )

            !=

            (
                pl.col(
                    "light_raw"
                )

                <

                pl.col(
                    "dark_threshold"
                )
            )
        )

        .height
    )


    # Número de experimentos.
    session_count = (

        environmental_df[
            "session_id"
        ]

        .n_unique()
    )


    quality_col1, quality_col2, quality_col3, quality_col4 = (
        st.columns(4)
    )


    render_kpi(
        quality_col1,
        "🗃️",
        translate("Curated Records"),
        str(
            environmental_df.height
        ),
        translate("Analytics-ready observations"),
    )


    render_kpi(
        quality_col2,
        "✅",
        translate("Valid DHT Readings"),
        str(
            valid_dht_count
        ),
        (
            f"{valid_dht_count / environmental_df.height * 100:.1f}% "
            f"{translate('valid')}"
        ),
    )


    render_kpi(
        quality_col3,
        "⚙️",
        translate("Edge Logic Errors"),
        str(
            logic_errors
        ),
        translate("LDR → state → LED consistency"),
    )


    render_kpi(
        quality_col4,
        "🧪",
        translate("Sessions"),
        str(
            session_count
        ),
        translate("Controlled experiments"),
    )


# ============================================================
# TAB 3 — ANOMALY DETECTION
# ============================================================

with anomaly_tab:

    render_section_label(
        translate("Machine Learning")
    )


    st.subheader(
        translate("Anomaly Detection")
    )


    render_description(
        translate(
            "Compare a robust statistical detector with Isolation Forest "
            "over controlled environmental events."
        )
    )


    # ========================================================
    # Sesión para ML
    # ========================================================

    anomaly_sessions = (

        anomaly_df[
            "session_id"
        ]

        .unique()

        .sort()

        .to_list()
    )


    anomaly_session = st.selectbox(

        translate("Session to analyze"),

        options=
            anomaly_sessions,

        format_func=
            format_session_name,

        key=
            "anomaly_session",
    )


    selected_anomalies = (

        anomaly_df

        .filter(
            pl.col(
                "session_id"
            )
            == anomaly_session
        )

        .sort(
            "device_timestamp"
        )
    )


    # ========================================================
    # Resultados de detección
    # ========================================================

    statistical_flags = int(

        selected_anomalies[
            "statistical_anomaly"
        ]

        .sum()
    )


    isolation_flags = int(

        selected_anomalies[
            "if_anomaly"
        ]

        .sum()
    )


    consensus_flags = int(

        selected_anomalies[
            "consensus_anomaly"
        ]

        .sum()
    )


    total = (
        selected_anomalies.height
    )


    # --------------------------------------------------------
    # Porcentajes
    # --------------------------------------------------------

    statistical_pct = (

        statistical_flags
        / total
        * 100

        if total

        else 0
    )


    isolation_pct = (

        isolation_flags
        / total
        * 100

        if total

        else 0
    )


    consensus_pct = (

        consensus_flags
        / total
        * 100

        if total

        else 0
    )


    anomaly_col1, anomaly_col2, anomaly_col3 = (
        st.columns(3)
    )


    render_kpi(
        anomaly_col1,
        "📐",
        translate("Statistical Detector"),
        f"{statistical_flags} {translate('flags')}",
        f"{statistical_pct:.1f}% {translate('of observations')}",
    )


    render_kpi(
        anomaly_col2,
        "🌲",
        "Isolation Forest",
        f"{isolation_flags} {translate('flags')}",
        f"{isolation_pct:.1f}% {translate('of observations')}",
    )


    render_kpi(
        anomaly_col3,
        "🎯",
        translate("Consensus"),
        f"{consensus_flags} {translate('flags')}",
        f"{consensus_pct:.1f}% {translate('detected by both')}",
    )


    st.divider()


    # ========================================================
    # Isolation Forest Score
    # ========================================================

    score_data = (

        to_plot_data(
            selected_anomalies,
            [
                "device_timestamp",
                "if_anomaly_score",
                "if_anomaly",
            ],
        )
    )


    score_fig = px.scatter(

        score_data,

        x=
            "device_timestamp",

        y=
            "if_anomaly_score",

        color=
            "if_anomaly",

        title=
            translate("Isolation Forest Anomaly Score"),

        labels={
            "device_timestamp":
                translate("Time"),

            "if_anomaly_score":
                translate("Anomaly score"),

            "if_anomaly":
                translate("Flagged"),
        },
    )


    style_figure(
        score_fig,
        height=390,
    )


    st.plotly_chart(
        score_fig,
        width="stretch",
    )


    # ========================================================
    # Statistical Score
    # ========================================================

    statistical_data = (

        to_plot_data(
            selected_anomalies,
            [
                "device_timestamp",
                "statistical_anomaly_score",
                "statistical_anomaly",
            ],
        )
    )


    statistical_fig = px.scatter(

        statistical_data,

        x=
            "device_timestamp",

        y=
            "statistical_anomaly_score",

        color=
            "statistical_anomaly",

        title=
            translate("Robust Statistical Anomaly Score"),

        labels={
            "device_timestamp":
                translate("Time"),

            "statistical_anomaly_score":
                translate("Robust anomaly score"),

            "statistical_anomaly":
                translate("Flagged"),
        },
    )


    # Threshold utilizado por nuestro
    # detector estadístico.
    statistical_fig.add_hline(

        y=3.5,

        line_dash="dash",

        annotation_text=
            translate("Threshold"),
    )


    style_figure(
        statistical_fig,
        height=390,
    )


    st.plotly_chart(
        statistical_fig,
        width="stretch",
    )


    # ========================================================
    # Consensus anomalies
    # ========================================================

    st.subheader(
        translate("Consensus Anomalies")
    )


    st.caption(
        translate(
            "Observations flagged simultaneously by the statistical "
            "detector and Isolation Forest."
        )
    )


    consensus_df = (

        selected_anomalies

        .filter(
            pl.col(
                "consensus_anomaly"
            )
        )

        .select(
            [
                "device_timestamp",
                "temperature_c",
                "humidity_pct",
                "light_raw",
                "temperature_change_magnitude",
                "light_change_magnitude",
                "statistical_anomaly_score",
                "if_anomaly_score",
            ]
        )

        .rename(
            {
                "device_timestamp":
                    translate("Timestamp"),

                "temperature_c":
                    f'{translate("Temperature")} (°C)',

                "humidity_pct":
                    f'{translate("Humidity")} (%)',

                "light_raw":
                    translate("Light"),

                "temperature_change_magnitude":
                    translate("Temp Change"),

                "light_change_magnitude":
                    translate("Light Change"),

                "statistical_anomaly_score":
                    translate("Statistical Score"),

                "if_anomaly_score":
                    translate("IF Score"),
            }
        )
    )


    if consensus_df.is_empty():

        st.success(
            translate(
                "No consensus anomalies detected for this session."
            )
        )

    else:

        st.dataframe(
            consensus_df,
            width="stretch",
            hide_index=True,
        )


# ============================================================
# Footer
# ============================================================

st.divider()


# Igual que hero y KPI:
# HTML completo en una sola línea.
footer_html = (
    '<div class="officepulse-footer">'
    '<span class="footer-line">'
    f'{translate("OfficePulse · Environmental IoT Analytics on AWS")}'
    '</span>'
    '<span class="footer-line">'
    'ESP32 · MQTT/TLS · AWS IoT Core · Amazon S3 · '
    'Polars · DuckDB · scikit-learn · Streamlit'
    '</span>'
    '</div>'
)


st.markdown(
    footer_html,
    unsafe_allow_html=True,
)
