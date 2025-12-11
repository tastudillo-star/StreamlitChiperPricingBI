import streamlit as st
import pandas as pd
from datetime import date, timedelta

from mySQLHelper import execute_mysql_query

st.title("Revisión y limpieza de datos – SIMPLE")

# ============================================
# Sidebar: parámetros básicos
# ============================================

st.sidebar.subheader("Parámetros")

# Competidores conocidos (ajusta según tu realidad)
COMPETIDORES = {
    0: "Todos los competidores",
    1: "Central Mayorista",
    2: "Alvi",
    3: "Otro competidor 3",
}

id_competidor_opt = st.sidebar.selectbox(
    "Competidor",
    options=list(COMPETIDORES.keys()),
    format_func=lambda x: f"{x} – {COMPETIDORES.get(x, 'Competidor')}",
    index=0,
)

# Rango de fechas simple
default_end = date.today()
default_start = default_end - timedelta(days=30)

rango_fechas = st.sidebar.date_input(
    "Rango de fechas (precio_competidor.fecha)",
    value=(default_start, default_end),
)

if isinstance(rango_fechas, (list, tuple)) and len(rango_fechas) == 2:
    fecha_desde, fecha_hasta = rango_fechas
else:
    fecha_desde = fecha_hasta = rango_fechas

if fecha_desde > fecha_hasta:
    fecha_desde, fecha_hasta = fecha_hasta, fecha_desde

# Umbrales configurables (por si quieres moverlos en el futuro)
umbral_superior = st.sidebar.number_input(
    "Umbral superior (ratio >)",
    min_value=0.1,
    max_value=100.0,
    value=2.0,
    step=0.1,
)

umbral_inferior = st.sidebar.number_input(
    "Umbral inferior (ratio <)",
    min_value=0.01,
    max_value=1.0,
    value=0.5,
    step=0.05,
)

st.markdown(
    f"**Rango fechas:** `{fecha_desde}` a `{fecha_hasta}`  \n"
    f"**Competidor:** {COMPETIDORES.get(id_competidor_opt)}  \n"
    f"**Criterio:** precio_chiper / precio_competidor_efectivo "
    f"> {umbral_superior} o < {umbral_inferior}"
)

st.markdown("---")

# ============================================
# Consulta SQL simplificada
# ============================================

@st.cache_data(show_spinner=True)
def load_outliers(
    fecha_desde_str: str,
    fecha_hasta_str: str,
    id_competidor_opt: int,
    umbral_sup: float,
    umbral_inf: float,
) -> pd.DataFrame:
    where_extra = ""
    if id_competidor_opt != 0:
        where_extra += f" AND pc.id_competidor = {id_competidor_opt}\n"

    query = f"""
    SELECT
        pc.id,
        pc.id_competidor,
        c.nombre AS nombre_competidor,
        pc.id_sku,
        s.sku,
        s.nombre AS nombre_sku,
        pc.fecha,
        pc.precio_lleno,
        pc.precio_descuento,
        vc.precio_bruto AS precio_bruto_chiper,
        COALESCE(pc.precio_descuento, pc.precio_lleno)
            AS precio_competidor_efectivo,
        (vc.precio_bruto / COALESCE(pc.precio_descuento, pc.precio_lleno))
            AS ratio_posicionamiento
    FROM precio_competidor AS pc
    JOIN competidor AS c
        ON c.id = pc.id_competidor
    JOIN sku AS s
        ON s.id = pc.id_sku
    LEFT JOIN ventas_chiper AS vc
        ON vc.id_sku = pc.id_sku
       AND vc.fecha  = pc.fecha
    WHERE
        vc.precio_bruto IS NOT NULL
        AND COALESCE(pc.precio_descuento, pc.precio_lleno) > 0
        AND DATE(pc.fecha) >= '{fecha_desde_str}'
        AND DATE(pc.fecha) <= '{fecha_hasta_str}'
        {where_extra}
        AND (
            (vc.precio_bruto / COALESCE(pc.precio_descuento, pc.precio_lleno)) > {umbral_sup}
            OR
            (vc.precio_bruto / COALESCE(pc.precio_descuento, pc.precio_lleno)) < {umbral_inf}
        )
    ORDER BY
        ratio_posicionamiento DESC;
    """
    return execute_mysql_query(query)


df = load_outliers(
    fecha_desde_str=fecha_desde.strftime("%Y-%m-%d"),
    fecha_hasta_str=fecha_hasta.strftime("%Y-%m-%d"),
    id_competidor_opt=id_competidor_opt,
    umbral_sup=umbral_superior,
    umbral_inf=umbral_inferior,
)

if df is None or df.empty:
    st.error("No se encontraron registros con posicionamientos raros bajo este criterio.")
    st.stop()

# Asegurar numéricos
for col in [
    "precio_lleno",
    "precio_descuento",
    "precio_bruto_chiper",
    "precio_competidor_efectivo",
    "ratio_posicionamiento",
]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

st.subheader("Registros detectados como outliers")

st.write(f"Total de filas: **{df.shape[0]}**  |  SKU distintos: **{df['id_sku'].nunique()}**")

st.dataframe(df, use_container_width=True, height=500)

# ============================================
# Selección manual de IDs a eliminar
# ============================================

st.markdown("---")
st.subheader("Selección de registros a eliminar")

ids_disponibles = df["id"].tolist()
ids_preseleccion = ids_disponibles  # si quieres partir con todos marcados

ids_seleccionados = st.multiselect(
    "IDs de `precio_competidor` a eliminar",
    options=ids_disponibles,
    default=ids_preseleccion,
)

st.write(f"Has seleccionado **{len(ids_seleccionados)}** registros.")

# Generar el DELETE listo para copiar
if ids_seleccionados:
    ids_str = ", ".join(str(int(x)) for x in ids_seleccionados)
    delete_sql = f"DELETE FROM precio_competidor WHERE id IN ({ids_str});"

    st.markdown("### SQL para eliminar estos registros")
    st.code(delete_sql, language="sql")
    st.info(
        "Copia este SQL y ejecútalo en tu cliente de base de datos "
        "(DBeaver, MySQL Workbench, etc.)."
    )
else:
    st.info("Selecciona al menos un ID para generar la sentencia DELETE.")
