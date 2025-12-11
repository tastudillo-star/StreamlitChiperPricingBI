# app.py
import streamlit as st

st.set_page_config(
    page_title="Pricing Chiper – BI",
    page_icon="📊",
    layout="wide"
)

st.title("Pricing Chiper – BI")
st.markdown("### Panel central de navegación")

st.write(
    """
    Bienvenido al panel de Pricing Chiper.

    Use el menú de páginas (barra lateral izquierda) para ir a:
    - SKUs: catálogo, imágenes.
    - Posicionamiento: relación precio Chiper vs competidor.
    - Configuración: pruebas de conexión y parámetros técnicos.
    """
)
