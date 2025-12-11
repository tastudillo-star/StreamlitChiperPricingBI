# pages/05_Pivot_Posicionamiento_Categoria.py
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
st.set_page_config(page_title="Reporte Posicionamiento", layout="wide")
st.title("Posicionamiento ponderado")

# ======================================================
# SIDEBAR: PARÁMETROS
# ======================================================
st.sidebar.subheader("Parámetros de ventana")

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
    "Fecha base (hasta cuándo mirar la ventana)",
    value=date.today(),
)

VENTANA_PRESETS = {
    "Últimos 5 días": 5,
    "Última semana (7 días)": 7,
    "Últimas 2 semanas (14 días)": 14,
    "Últimas 3 semanas (21 días)": 21,
    "Último mes (30 días)": 30,
    "Últimos 3 meses (90 días)": 90,
    "Personalizado": None,
}

preset_label = st.sidebar.selectbox(
    "Ventana de tiempo",
    options=list(VENTANA_PRESETS.keys()),
    index=4,  # por defecto: 30 días
)

if VENTANA_PRESETS[preset_label] is None:
    ventana = st.sidebar.number_input(
        "Ventana de días hacia atrás",
        min_value=1,
        max_value=365,
        value=30,
        step=1,
    )
else:
    ventana = VENTANA_PRESETS[preset_label]

st.markdown(
    f"**Ventana seleccionada:** {preset_label} "
    f"(`{ventana}` días) hasta {fecha_actual.strftime('%Y-%m-%d')} (incluido).  \n"
    f"**Competidor:** {COMPETIDORES.get(id_competidor, id_competidor)}"
)

# ======================================================
# CARGA DE DATOS DESDE MYSQL
# ======================================================
@st.cache_data(show_spinner=True)
def load_posicionamiento_categoria(
    id_competidor: int,
    fecha_str: str,
    ventana: int,
) -> pd.DataFrame:
    """
    Ejecuta la consulta de ventana (competidor + Chiper)
    y devuelve un DataFrame a nivel de SKU.
    """
    query = f"""
    WITH
    params AS (
      SELECT
        {id_competidor}             AS id_competidor,
        CAST('{fecha_str}' AS DATE) AS fecha_actual,
        {ventana}                   AS dias_ventana
    ),

    -- 1) Base de precios de competidor
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
      WHERE
          (p.id_competidor IS NULL OR pc.id_competidor = p.id_competidor)
          AND DATE(pc.fecha) >= DATE_SUB(p.fecha_actual, INTERVAL p.dias_ventana DAY)
          AND DATE(pc.fecha) <= p.fecha_actual
          AND (pc.precio_lleno IS NOT NULL OR pc.precio_descuento IS NOT NULL)
    ),

    -- 2) Agregado de precios competidor por ventana
    agg_competidor AS (
      SELECT
          p.fecha_actual             AS fecha_actual,
          p.dias_ventana             AS dias_ventana,
          bc.id_competidor,
          bc.id_sku,
          AVG(bc.precio_lleno)              AS precio_lleno_prom_ventana,
          AVG(bc.precio_descuento)          AS precio_descuento_prom_ventana,
          AVG(bc.precio_competidor_min_dia) AS precio_competidor_min_prom_ventana
      FROM base_competidor bc
      CROSS JOIN params p
      GROUP BY
          p.fecha_actual,
          p.dias_ventana,
          bc.id_competidor,
          bc.id_sku
    ),

    -- 3) Base de ventas Chiper
    base_chiper AS (
      SELECT
          vc.id_sku,
          DATE(vc.fecha)   AS fecha,
          vc.precio_bruto,
          vc.venta_neta
      FROM ventas_chiper vc
      JOIN params p
      WHERE
          DATE(vc.fecha) >= DATE_SUB(p.fecha_actual, INTERVAL p.dias_ventana DAY)
          AND DATE(vc.fecha) <= p.fecha_actual
          AND vc.precio_bruto IS NOT NULL
    ),

    -- 4) Agregado de Chiper por ventana
    agg_chiper AS (
      SELECT
          p.fecha_actual       AS fecha_actual,
          p.dias_ventana       AS dias_ventana,
          bc.id_sku,
          SUM(bc.venta_neta)   AS sum_venta_neta,
          AVG(bc.precio_bruto) AS precio_chiper_prom_ventana
      FROM base_chiper bc
      CROSS JOIN params p
      GROUP BY
          p.fecha_actual,
          p.dias_ventana,
          bc.id_sku
    ),

    -- 5) Total de SKUs de Chiper en la ventana (para KPI de representatividad)
    chiper_skus AS (
      SELECT COUNT(DISTINCT bc.id_sku) AS total_skus_chiper
      FROM base_chiper bc
    ),

    -- 6) Join competidor + Chiper + info de SKU/categoría/macro/proveedor
    joined AS (
      SELECT
          ac.fecha_actual,
          ac.dias_ventana,
          ac.id_competidor,
          ac.id_sku,
          s.sku,
          mc.nombre AS macro,
          c.nombre  AS categoria,
          pr.nombre AS proveedor,
          s.nombre  AS nombre,
          ac.precio_lleno_prom_ventana,
          ac.precio_descuento_prom_ventana,
          ac.precio_competidor_min_prom_ventana,
          ach.sum_venta_neta,
          ach.precio_chiper_prom_ventana
      FROM agg_competidor ac
      JOIN sku s
        ON s.id = ac.id_sku
      LEFT JOIN categoria c
        ON c.id = s.id_categoria
      LEFT JOIN macro_categoria mc
        ON mc.id = c.id_macro
      LEFT JOIN proveedor pr
        ON pr.id = s.id_proveedor
      LEFT JOIN agg_chiper ach
        ON ach.id_sku = ac.id_sku
        AND ach.fecha_actual = ac.fecha_actual
        AND ach.dias_ventana = ac.dias_ventana
    ),

    -- 7) Cálculos de posicionamiento y peso de venta
    final AS (
      SELECT
          j.*,
          cs.total_skus_chiper,

          j.precio_competidor_min_prom_ventana AS precio_competidor_min,

          CASE
            WHEN j.precio_chiper_prom_ventana IS NULL THEN NULL
            WHEN j.precio_competidor_min_prom_ventana IS NULL THEN NULL
            WHEN j.precio_competidor_min_prom_ventana = 0 THEN NULL
            ELSE j.precio_chiper_prom_ventana / j.precio_competidor_min_prom_ventana
          END AS posicionamiento,

          CASE
            WHEN SUM(j.sum_venta_neta) OVER () = 0 THEN NULL
            ELSE j.sum_venta_neta / SUM(j.sum_venta_neta) OVER ()
          END AS peso_venta

      FROM joined j
      CROSS JOIN chiper_skus cs
    )

    SELECT
        sku,
        macro,
        categoria,
        proveedor,
        nombre,
        precio_chiper_prom_ventana    AS precio_chiper,
        precio_lleno_prom_ventana     AS precio_lleno_competidor,
        precio_descuento_prom_ventana AS precio_descuento_competidor,
        sum_venta_neta                AS venta_neta,
        posicionamiento,
        peso_venta,
        total_skus_chiper
    FROM final
    ORDER BY
        id_sku;
    """
    return execute_mysql_query(query)


df = load_posicionamiento_categoria(
    id_competidor=id_competidor,
    fecha_str=fecha_actual.strftime("%Y-%m-%d"),
    ventana=ventana,
)

if df is None or df.empty:
    st.error("No se encontraron datos para la ventana seleccionada.")
    st.stop()

# Asegurar columnas numéricas
for col in [
    "precio_chiper",
    "precio_lleno_competidor",
    "precio_descuento_competidor",
    "venta_neta",
    "posicionamiento",
    "peso_venta",
]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ======================================================
# KPI DE REPRESENTATIVIDAD (ANTES DE FILTRAR RANGO 0.5–2)
# ======================================================
if "total_skus_chiper" in df.columns:
    try:
        total_skus_chiper = int(df["total_skus_chiper"].iloc[0])
    except Exception:
        total_skus_chiper = df["total_skus_chiper"].iloc[0]
else:
    # Fallback: si por alguna razón no viene la columna, usamos SKUs presentes en df
    total_skus_chiper = df["sku"].nunique()

skus_con_posicionamiento = df[df["posicionamiento"].notna()]["sku"].nunique()

if total_skus_chiper:
    representatividad = skus_con_posicionamiento / total_skus_chiper
else:
    representatividad = np.nan

# A partir de aquí, ya no consideramos SKUs sin posicionamiento válido
df = df[df["posicionamiento"].notna()]

# ======================================================
# FILTRO POR POSICIONAMIENTO (0.5–2)
# ======================================================
df = df[
    (df["posicionamiento"] >= 0.5)
    & (df["posicionamiento"] <= 2)
]

if df.empty:
    st.error(
        "No se encontraron datos luego de aplicar el filtro de posicionamiento "
        "(solo se consideran SKUs con posicionamiento entre 0.5 y 2)."
    )
    st.stop()

# ======================================================
# AGREGACIÓN A NIVEL CATEGORÍA (PROMEDIO PONDERADO)
# ======================================================
def agg_categoria(grp: pd.DataFrame) -> pd.Series:
    """
    Agrega por macro/categoría usando:
    - venta_categoria: suma de venta_neta
    - peso_venta_categoria: suma de peso_venta
    - posicionamiento_pond: promedio ponderado por peso_venta
    """
    peso_total = grp["peso_venta"].sum(skipna=True)

    if peso_total and not np.isclose(peso_total, 0):
        pos_pond = (grp["posicionamiento"] * grp["peso_venta"]).sum(skipna=True) / peso_total
    else:
        pos_pond = np.nan

    return pd.Series(
        {
            "venta_categoria": grp["venta_neta"].sum(skipna=True),
            "posicionamiento_pond": pos_pond,
            "peso_venta_categoria": peso_total,
        }
    )


df_cat = (
    df.groupby(["macro", "categoria"], dropna=False)
    .apply(agg_categoria)
    .reset_index()
)

df_ag = df_cat.rename(
    columns={
        "macro": "macro_categoria",
        "categoria": "categoria",
    }
)

cols_order = [
    "macro_categoria",
    "categoria",
    "venta_categoria",
    "peso_venta_categoria",
    "posicionamiento_pond",
]
df_ag = df_ag[[c for c in cols_order if c in df_ag.columns]]

# ======================================================
# KPIs
# ======================================================
st.subheader("Resumen de categorías")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        "Venta total (todas las categorías)",
        f"${df_cat['venta_categoria'].sum():,.0f}",
    )
with col2:
    st.metric(
        "Nº categorías con datos",
        f"{df_cat.shape[0]}",
    )
with col3:
    st.metric(
        "Posicionamiento ponderado total",
        f"{((df['peso_venta'] * df['posicionamiento']).sum())/df['peso_venta'].sum():.2%}",
    )
with col4:
    st.metric(
        "Representatividad SKUs",
        f"{representatividad:.2%}" if not np.isnan(representatividad) else "N/A",
    )

st.markdown("---")

# ======================================================
# TABLA PIVOTE DESPLEGABLE (MACRO → CATEGORÍA → SKU)
# ======================================================
st.subheader("Tabla pivote desplegable – Macro → Categoría → SKU")

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

    # Venta SKU como CLP, sin decimales
    gb.configure_column(
        "venta_neta",
        header_name="Venta SKU",
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
        header_name="Posicionamiento SKU",
        type=["numericColumn"],
        aggFunc="weightedAvg",  # promedio ponderado por peso_venta
        valueFormatter="value == null ? '' : (Number(value) * 100).toFixed(2) + '%'",
        cellStyle=posicionamiento_cell_style,
    )

    # Estilo para peso de venta (intensidad según peso)
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
        header_name="Peso venta",
        type=["numericColumn"],
        aggFunc="sum",  # suma de pesos dentro del grupo
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
        enable_enterprise_modules=True,  # necesario para rowGroup
        height=600,
    )

# ======================================================
# TABLA DETALLADA POR CATEGORÍA
# ======================================================
st.subheader("Detalle plano por categoría")

st.dataframe(
    df_ag[[
        "macro_categoria",
        "categoria",
        "venta_categoria",
        "peso_venta_categoria",
        "posicionamiento_pond",
    ]],
    use_container_width=True,
    height=400,
)

# ======================================================
# DETALLE POR SKU
# ======================================================
with st.expander("Ver detalle por SKU"):
    st.dataframe(
        df[[
            "sku",
            "nombre",
            "macro",
            "categoria",
            "proveedor",
            "precio_chiper",
            "precio_lleno_competidor",
            "precio_descuento_competidor",
            "venta_neta",
            "posicionamiento",
            "peso_venta",
        ]].sort_values("venta_neta", ascending=False),
        use_container_width=True,
        height=500,
    )
