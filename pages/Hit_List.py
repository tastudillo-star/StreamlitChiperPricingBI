# pages/04_Top20_Ventas.py
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date, timedelta

from mySQLHelper import execute_mysql_query

st.title("Top 20 productos por venta neta")

st.sidebar.subheader("Parámetros de periodo")

# Rango de fechas por defecto: últimos 30 días
default_end = date.today()
default_start = default_end - timedelta(days=30)

rango_fechas = st.sidebar.date_input(
    "Rango de fechas",
    value=(default_start, default_end)
)

# Manejo de rango (Streamlit devuelve una tupla de 2 fechas)
if isinstance(rango_fechas, (list, tuple)) and len(rango_fechas) == 2:
    dfrom, dto = rango_fechas
else:
    # Si el usuario solo selecciona una fecha, usamos esa como inicio y fin
    dfrom = dto = rango_fechas

# Asegurar que inicio <= fin
if dfrom > dto:
    dfrom, dto = dto, dfrom

st.markdown(
    f"**Periodo seleccionado:** {dfrom.strftime('%Y-%m-%d')} → {dto.strftime('%Y-%m-%d')}"
)


@st.cache_data(show_spinner=True)
def load_top_20_ventas(dfrom_str: str, dto_str: str) -> pd.DataFrame:
    """
    Consulta el Top 20 productos por venta neta en el periodo indicado.
    Usa la estructura de daily_sku que compartiste.
    """
    query = f"""
    WITH
    params AS (
      SELECT
        CAST('{dfrom_str}' AS DATE) AS dfrom,
        CAST('{dto_str}'   AS DATE) AS dto
    ),
    daily_sku AS (
      SELECT 
        DATE(v.fecha)                      AS date,
        v.id_sku                           AS sku,
        SUM(v.venta_neta)                  AS venta,
        SUM(v.cantidad)                    AS unidades,

        -- Precio bruto promedio del día (ponderado por unidades)
        SUM(v.precio_bruto * v.cantidad) 
          / NULLIF(SUM(v.cantidad), 0)     AS precio_bruto_prom_dia,

        -- Margen total (front + back) del día, ponderado por venta
        SUM( (v.front + v.back) * v.venta_neta )
          / NULLIF(SUM(v.venta_neta), 0)   AS margen_front_back_prom_dia,

        -- Precios competidor por día
        AVG(pc.precio_lleno)               AS precio_lleno_dia,
        AVG(pc.precio_descuento)           AS precio_descuento_dia
      FROM ventas_chiper v
      CROSS JOIN params p
      LEFT JOIN precio_competidor pc
        ON pc.id_sku = v.id_sku
       AND DATE(pc.fecha) = DATE(v.fecha)
       AND pc.id_competidor = 1          -- opcional / fijo por ahora
      WHERE v.fecha >= p.dfrom
        AND v.fecha <= p.dto
      GROUP BY DATE(v.fecha), v.id_sku
    )
    SELECT
        d.sku,
        s.nombre                                    AS nombre_sku,
        c.nombre                                    AS categoria,
        mc.nombre                                   AS macro_categoria,
        pvd.nombre                                  AS proveedor,

        SUM(d.venta)                                AS venta_total_periodo,
        SUM(d.unidades)                             AS unidades_total_periodo,

        -- Precio bruto promedio ponderado por la venta de cada día
        SUM(d.precio_bruto_prom_dia * d.venta)
          / NULLIF(SUM(d.venta), 0)                 AS precio_bruto_prom_pond,

        -- Margen (front + back) promedio ponderado por la venta de cada día
        SUM(d.margen_front_back_prom_dia * d.venta)
          / NULLIF(SUM(d.venta), 0)                 AS margen_front_back_prom_pond,

        -- Precio lleno competidor promedio ponderado por venta Chiper
        SUM(
          CASE 
            WHEN d.precio_lleno_dia IS NOT NULL 
            THEN d.precio_lleno_dia * d.venta 
          END
        )
          / NULLIF(
              SUM(
                CASE 
                  WHEN d.precio_lleno_dia IS NOT NULL 
                  THEN d.venta 
                END
              ),
              0
            )                                       AS precio_lleno_prom_pond,

        -- Precio descuento competidor promedio ponderado
        SUM(
          CASE 
            WHEN d.precio_descuento_dia IS NOT NULL 
            THEN d.precio_descuento_dia * d.venta 
          END
        )
          / NULLIF(
              SUM(
                CASE 
                  WHEN d.precio_descuento_dia IS NOT NULL 
                  THEN d.venta 
                END
              ),
              0
            )                                       AS precio_descuento_prom_pond
    FROM daily_sku d
    LEFT JOIN sku s
        ON s.id = d.sku
    LEFT JOIN categoria c
        ON c.id = s.id_categoria
    LEFT JOIN macro_categoria mc
        ON mc.id = c.id_macro
    LEFT JOIN proveedor pvd
        ON pvd.id = s.id_proveedor
    GROUP BY 
        d.sku, s.nombre, c.nombre, mc.nombre, pvd.nombre
    ORDER BY venta_total_periodo DESC
      LIMIT 20;
    """
    return execute_mysql_query(query)


# Ejecutar consulta
df_top = load_top_20_ventas(
    dfrom.strftime("%Y-%m-%d"),
    dto.strftime("%Y-%m-%d")
)

if df_top is None or df_top.empty:
    st.error("No se encontraron ventas en el periodo seleccionado.")
    st.stop()

# ============================
# KPIs simples
# ============================
st.subheader("Resumen del Top 20")

col1, col2, col3 = st.columns(3)
with col1:
    st.metric(
        "Venta total Top 20",
        f"${df_top['venta_total_periodo'].sum():,.0f}"
    )
with col2:
    st.metric(
        "Unidades totales Top 20",
        f"{df_top['unidades_total_periodo'].sum():,.0f}"
    )
with col3:
    st.metric(
        "Venta promedio por SKU (Top 20)",
        f"${df_top['venta_total_periodo'].mean():,.0f}"
    )

st.markdown("---")

# ============================
# Gráfico de barras – venta_total_periodo
# ============================
st.subheader("Ranking por venta neta en el periodo")

# Ordenar por venta para que el gráfico quede consistente
df_plot = df_top.sort_values("venta_total_periodo", ascending=False)

fig = px.bar(
    df_plot,
    x="nombre_sku",
    y="venta_total_periodo",
    color="macro_categoria",
    title="Top 20 productos por venta neta",
    height=700  # alto en píxeles
)
fig.update_layout(
    xaxis_title="SKU",
    yaxis_title="Venta neta periodo",
    xaxis_tickangle=-45
)
st.plotly_chart(fig, use_container_width=True)

# ============================
# Tabla detallada
# ============================
st.subheader("Detalle Top 20")

# Reordenar columnas para lectura
cols_order = [
    "sku",
    "nombre_sku",
    "macro_categoria",
    "categoria",
    "proveedor",
    "venta_total_periodo",
    "unidades_total_periodo",
    "precio_bruto_prom_pond",
    "margen_front_back_prom_pond",
    "precio_lleno_prom_pond",
    "precio_descuento_prom_pond",
]
cols_presentes = [c for c in cols_order if c in df_top.columns]

st.dataframe(
    df_top[cols_presentes],
    use_container_width=True,
    height=500
)
