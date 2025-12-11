# pages/Posicionamiento_Hoy.py
from __future__ import annotations

import streamlit as st
import pandas as pd
import numpy as np
from datetime import date

from mySQLHelper import execute_mysql_query  # Cliente MySQL

# Intentar importar st-aggrid
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
    AGGRID_AVAILABLE = True
except ImportError:
    AGGRID_AVAILABLE = False

# ======================================================
# CONFIGURACIÓN GENERAL
# ======================================================
st.set_page_config(page_title="Posicionamiento diario", layout="wide")
st.title("Posicionamiento diario – Tabla pivote")

# ======================================================
# SIDEBAR: PARÁMETROS
# ======================================================
st.sidebar.subheader("Parámetros del día")

COMPETIDORES = {
    1: "Central Mayorista",
    2: "Alvi",
    3: "Otro competidor 3",
}

id_competidor = st.sidebar.selectbox(
    "Competidor",
    options=list(COMPETIDORES.keys()),
    format_func=lambda x: f"{x} – {COMPETIDORES.get(x, 'Competidor')}",
    index=0,
)

fecha_actual = st.sidebar.date_input(
    "Fecha de análisis",
    value=date.today(),
)

st.markdown(
    f"**Fecha seleccionada:** {fecha_actual.strftime('%Y-%m-%d')}  \n"
    f"**Competidor:** {COMPETIDORES.get(id_competidor, id_competidor)}"
)

# ======================================================
# CARGA DE DATOS DESDE MYSQL (SOLO ESE DÍA)
# ======================================================
@st.cache_data(show_spinner=True)
def load_posicionamiento_dia(
    id_competidor: int,
    fecha_str: str,
) -> pd.DataFrame:
    """
    Devuelve un DataFrame a nivel SKU para un solo día:
    - precios diarios competidor y Chiper
    - venta_neta diaria
    - posicionamiento diario
    Solo incluye SKUs con datos de competidor y Chiper (para poder calcular posicionamiento).
    """
    query = f"""
    WITH
    params AS (
      SELECT
        {id_competidor}             AS id_competidor,
        CAST('{fecha_str}' AS DATE) AS fecha_ref
    ),

    -- 1) Base de precios de competidor (solo ese día)
    base_competidor AS (
      SELECT
          pc.id_sku,
          pc.id_competidor,
          DATE(pc.fecha)     AS fecha,
          pc.precio_lleno,
          pc.precio_descuento,
          CASE
            WHEN pc.precio_lleno IS NULL
                 AND pc.precio_descuento IS NULL THEN NULL
            WHEN pc.precio_lleno IS NULL THEN pc.precio_descuento
            WHEN pc.precio_descuento IS NULL THEN pc.precio_lleno
            ELSE LEAST(pc.precio_lleno, pc.precio_descuento)
          END AS precio_competidor_min_dia
      FROM precio_competidor pc
      JOIN params p
        ON (p.id_competidor IS NULL OR pc.id_competidor = p.id_competidor)
      WHERE
          DATE(pc.fecha) = p.fecha_ref
          AND (pc.precio_lleno IS NOT NULL OR pc.precio_descuento IS NOT NULL)
    ),

    -- 2) Agregado competidor por SKU en el día
    agg_competidor_dia AS (
      SELECT
          bc.id_sku,
          bc.id_competidor,
          bc.fecha,
          AVG(bc.precio_lleno)              AS precio_lleno_dia,
          AVG(bc.precio_descuento)          AS precio_descuento_dia,
          AVG(bc.precio_competidor_min_dia) AS precio_competidor_min_dia
      FROM base_competidor bc
      GROUP BY
          bc.id_sku,
          bc.id_competidor,
          bc.fecha
    ),

    -- 3) Base de ventas Chiper para ese día
    base_chiper AS (
      SELECT
          vc.id_sku,
          DATE(vc.fecha)   AS fecha,
          vc.precio_bruto,
          vc.venta_neta
      FROM ventas_chiper vc
      JOIN params p
      WHERE
          DATE(vc.fecha) = p.fecha_ref
          AND vc.precio_bruto IS NOT NULL
    ),

    -- 4) Agregado Chiper por SKU en el día
    agg_chiper_dia AS (
      SELECT
          bc.id_sku,
          bc.fecha,
          SUM(bc.venta_neta)   AS venta_neta_dia,
          AVG(bc.precio_bruto) AS precio_chiper_dia
      FROM base_chiper bc
      GROUP BY
          bc.id_sku,
          bc.fecha
    ),

    -- 5) Join competidor + Chiper + info de SKU/categoría/macro/proveedor
    joined AS (
      SELECT
          acd.fecha,
          acd.id_sku,
          s.sku,
          mc.nombre AS macro,
          c.nombre  AS categoria,
          pr.nombre AS proveedor,
          s.nombre  AS nombre,
          acd.precio_lleno_dia,
          acd.precio_descuento_dia,
          acd.precio_competidor_min_dia,
          achd.venta_neta_dia,
          achd.precio_chiper_dia
      FROM agg_competidor_dia acd
      JOIN agg_chiper_dia achd
        ON acd.id_sku = achd.id_sku
       AND acd.fecha  = achd.fecha
      JOIN sku s
        ON s.id = acd.id_sku
      LEFT JOIN categoria c
        ON c.id = s.id_categoria
      LEFT JOIN macro_categoria mc
        ON mc.id = c.id_macro
      LEFT JOIN proveedor pr
        ON pr.id = s.id_proveedor
    ),

    -- 6) Cálculo de posicionamiento diario
    final AS (
      SELECT
          j.*,
          CASE
            WHEN j.precio_chiper_dia IS NULL THEN NULL
            WHEN j.precio_competidor_min_dia IS NULL THEN NULL
            WHEN j.precio_competidor_min_dia = 0 THEN NULL
            ELSE j.precio_chiper_dia / j.precio_competidor_min_dia
          END AS posicionamiento
      FROM joined j
    )

    SELECT
        fecha,
        sku,
        macro,
        categoria,
        proveedor,
        nombre,
        precio_chiper_dia            AS precio_chiper,
        precio_lleno_dia             AS precio_lleno_competidor,
        precio_descuento_dia         AS precio_descuento_competidor,
        venta_neta_dia               AS venta_neta,
        posicionamiento
    FROM final
    ORDER BY
        sku;
    """
    return execute_mysql_query(query)


df = load_posicionamiento_dia(
    id_competidor=id_competidor,
    fecha_str=fecha_actual.strftime("%Y-%m-%d"),
)

if df is None or df.empty:
    st.error("No se encontraron datos para el día seleccionado.")
    st.stop()

# Normalizar fecha a date
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date

# Asegurar tipos numéricos
for col in [
    "precio_chiper",
    "precio_lleno_competidor",
    "precio_descuento_competidor",
    "venta_neta",
    "posicionamiento",
]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Por seguridad, filtramos solo la fecha seleccionada (deberían ser todas)
df = df[df["fecha"] == fecha_actual]

if df.empty:
    st.error("No hay datos para esa fecha después de limpiar la información.")
    st.stop()

# ======================================================
# CÁLCULO DE PESO DE VENTA DEL DÍA
# ======================================================
df["venta_neta"] = df["venta_neta"].fillna(0)
total_venta_dia = df["venta_neta"].sum()

if total_venta_dia > 0:
    df["peso_venta"] = df["venta_neta"] / total_venta_dia
else:
    df["peso_venta"] = 0.0

# ======================================================
# FILTRO DE POSICIONAMIENTO (0.5–2)
# ======================================================
df = df[df["posicionamiento"].notna()]
df = df[
    (df["posicionamiento"] >= 0.5)
    & (df["posicionamiento"] <= 2)
]

if df.empty:
    st.error(
        "No se encontraron SKUs con posicionamiento entre 0.5 y 2 para ese día."
    )
    st.stop()

# ======================================================
# KPIs BÁSICOS DEL DÍA
# ======================================================
st.subheader("KPIs del día")

pos_pond_dia = np.nan
if total_venta_dia > 0:
    pos_pond_dia = (df["posicionamiento"] * df["peso_venta"]).sum()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric(
        "Venta total día",
        f"${total_venta_dia:,.0f}",
    )
with col2:
    st.metric(
        "Nº categorías con datos",
        f"{df['categoria'].nunique()}",
    )
with col3:
    st.metric(
        "Posicionamiento ponderado (día)",
        f"{pos_pond_dia*100:.2f}%" if not np.isnan(pos_pond_dia) else "N/A",
    )

st.markdown("---")

# ======================================================
# TABLA PIVOTE DESPLEGABLE (MACRO → CATEGORÍA → SKU)
# ======================================================
st.subheader("Tabla pivote diaria – Macro → Categoría → SKU")

df_pivot = df.rename(
    columns={
        "macro": "macro_categoria",
        "nombre": "nombre_sku",
    }
)[[
    "macro_categoria",
    "categoria",
    "nombre_sku",
    "venta_neta",
    "posicionamiento",
    "peso_venta",
]]

if not AGGRID_AVAILABLE:
    st.info(
        "Para usar la tabla pivote desplegable necesitas `streamlit-aggrid`.\n"
        "Instala con: `pip install streamlit-aggrid`.\n\n"
        "Mostrando tabla estática como alternativa."
    )
    st.dataframe(df_pivot, use_container_width=True, height=600)
else:
    # --- Función de agregación de promedio ponderado para AG Grid ---
    weighted_avg_agg = JsCode("""
        function(params) {
            var field = params.column.getColId();
            var rowNode = params.rowNode;

            if (!rowNode || !rowNode.allLeafChildren) {
                return null;
            }

            var children = rowNode.allLeafChildren;
            var sum = 0.0;
            var weightSum = 0.0;

            for (var i = 0; i < children.length; i++) {
                var data = children[i].data;
                if (!data) continue;

                var v = data[field];
                var w = data['peso_venta'];

                if (v == null || w == null) continue;

                sum += v * w;
                weightSum += w;
            }

            if (weightSum === 0) {
                return null;
            }

            return sum / weightSum;
        }
    """)

    gb = GridOptionsBuilder.from_dataframe(df_pivot)

    # Registrar función de agregación personalizada
    gb.configure_grid_options(
        aggFuncs={
            "weightedAvg": weighted_avg_agg,
        }
    )

    gb.configure_default_column(
        groupable=True,
        value=True,
        enableRowGroup=True,
        sortable=True,
        filter=True,
        editable=False,
        resizable=True,
    )

    # Jerarquía: Macro → Categoría → SKU
    gb.configure_column("macro_categoria", rowGroup=True, hide=True)
    gb.configure_column("categoria", rowGroup=True, hide=True)
    gb.configure_column("nombre_sku", rowGroup=True, hide=True)

    # Venta SKU día como CLP, sin decimales
    gb.configure_column(
        "venta_neta",
        header_name="Venta SKU (día)",
        type=["numericColumn"],
        aggFunc="sum",
        valueFormatter=(
            "value == null ? '' : "
            "value.toLocaleString('es-CL', {minimumFractionDigits: 0, maximumFractionDigits: 0})"
        ),
    )

    # Estilo condicional para posicionamiento (gradiente verde/rojo)
    posicionamiento_cell_style = JsCode("""
        function(params) {
            var v = null;

            if (params.data && params.data.posicionamiento != null) {
                v = params.data.posicionamiento;
            } else if (params.value != null) {
                v = params.value;
            }

            if (v == null) return {};

            var minVal = 0.5;
            var midVal = 1.0;
            var maxVal = 2.0;

            function clamp(x, a, b) { return Math.max(a, Math.min(b, x)); }
            function lerp(a, b, t)  { return a + (b - a) * t; }

            function hexToRgb(hex) {
                var h = hex.replace('#','');
                var bigint = parseInt(h, 16);
                return {
                    r: (bigint >> 16) & 255,
                    g: (bigint >> 8) & 255,
                    b: bigint & 255
                };
            }

            var red   = hexToRgb('#F8696B');
            var green = hexToRgb('#63BE7B');
            var white = {r: 255, g: 255, b: 255};
            var c;

            if (v < midVal) {
                var t = clamp((midVal - v) / (midVal - minVal), 0, 1);
                c = {
                    r: Math.round(lerp(white.r, green.r, t)),
                    g: Math.round(lerp(white.g, green.g, t)),
                    b: Math.round(lerp(white.b, green.b, t))
                };
            } else if (v > midVal) {
                var t = clamp((v - midVal) / (maxVal - midVal), 0, 1);
                c = {
                    r: Math.round(lerp(white.r, red.r, t)),
                    g: Math.round(lerp(white.g, red.g, t)),
                    b: Math.round(lerp(white.b, red.b, t))
                };
            } else {
                c = white;
            }

            var bg = 'rgb(' + c.r + ',' + c.g + ',' + c.b + ')';
            return {
                'backgroundColor': bg,
                'color': 'black'
            };
        }
    """)

    gb.configure_column(
        "posicionamiento",
        header_name="Posicionamiento SKU (día)",
        type=["numericColumn"],
        aggFunc="weightedAvg",  # promedio ponderado por peso_venta
        valueFormatter="value == null ? '' : (Number(value) * 100).toFixed(2) + '%'",
        cellStyle=posicionamiento_cell_style,
    )

    # Estilo para peso de venta
    peso_venta_cell_style = JsCode("""
        function(params) {
            var v = (params.data && params.data.peso_venta != null)
                    ? params.data.peso_venta
                    : params.value;

            if (v == null) return {};

            var scale = v / 0.05;  // saturación hasta 5%
            scale = Math.max(0, Math.min(1, scale));

            var yellow = {r: 246, g: 227, b: 122};  // #F6E37A
            var white  = {r: 255, g: 255, b: 255};

            function lerp(a, b, t) { return a + (b - a) * t; }

            var c = {
                r: Math.round(lerp(white.r, yellow.r, scale)),
                g: Math.round(lerp(white.g, yellow.g, scale)),
                b: Math.round(lerp(white.b, yellow.b, scale))
            };

            return {
                'backgroundColor': 'rgb(' + c.r + ',' + c.g + ',' + c.b + ')',
                'color': 'black'
            };
        }
    """)

    gb.configure_column(
        "peso_venta",
        header_name="Peso venta (día)",
        type=["numericColumn"],
        aggFunc="sum",  # suma dentro de macro/categoría
        valueFormatter="value == null ? '' : (Number(value) * 100).toFixed(2) + '%'",
        cellStyle=peso_venta_cell_style,
    )

    grid_options = gb.build()
    grid_options["groupDefaultExpanded"] = 0
    grid_options["autoGroupColumnDef"] = {
        "headerName": "Macro / Categoría / SKU",
    }

    AgGrid(
        df_pivot,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.NO_UPDATE,
        allow_unsafe_jscode=True,
        enable_enterprise_modules=True,
        height=600,
    )
