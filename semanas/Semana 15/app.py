import streamlit as st
import pandas as pd
import plotly.express as px

# ======================================================
# CONFIGURACIÓN DE LA PÁGINA
# ======================================================

st.set_page_config(
    page_title="Dashboard AgroTech",
    layout="wide",
    page_icon="🌾"
)

st.title("🌾 Cuadro de Mando Integral - Analítica AgroTech")
st.markdown("### Monitoreo de Precios Agrícolas, Clima y Macroeconomía")
st.markdown("---")


# ======================================================
# CARGA DE DATOS
# ======================================================

@st.cache_data
def cargar_datos():
    try:
        df_local = pd.read_csv("datos_agrotech_dashboard.csv")

        # Convertir nombres de columnas a minúsculas
        df_local.columns = df_local.columns.str.lower()

        # Convertir fecha
        if "fecha" in df_local.columns:
            df_local["fecha"] = pd.to_datetime(df_local["fecha"])

        return df_local

    except Exception as e:
        st.error(f"Error cargando CSV: {e}")
        return pd.DataFrame()


df = cargar_datos()

# ======================================================
# DASHBOARD
# ======================================================

if not df.empty:

    st.sidebar.header("Filtros del Ecosistema")

    if "producto" in df.columns:

        df = df[df["producto"].notna()]

        productos_disponibles = sorted(
            df["producto"].unique().tolist()
        )

    else:
        productos_disponibles = ["Sin Datos"]

    producto_sel = st.sidebar.selectbox(
        "Selecciona un Producto:",
        productos_disponibles
    )

    # Filtrar producto
    if "producto" in df.columns:
        df_filtrado = (
            df[df["producto"] == producto_sel]
            .sort_values(by="fecha")
        )
    else:
        df_filtrado = df

    # ======================================================
    # PESTAÑAS
    # ======================================================

    tab_est, tab_tac, tab_op = st.tabs(
        [
            "📈 Nivel Estratégico",
            "📊 Nivel Táctico",
            "⚡ Nivel Operacional"
        ]
    )

    # ======================================================
    # NIVEL ESTRATÉGICO
    # ======================================================

    with tab_est:

        st.header("📈 KPI Estratégico: Precio Promedio por Mes")

        if "mes" in df_filtrado.columns and "precio" in df_filtrado.columns:

            df_mes = (
                df_filtrado
                .groupby("mes")["precio"]
                .mean()
                .reset_index()
                .sort_values("mes")
            )

            precio_promedio = df_filtrado["precio"].mean()

            st.metric(
                "Precio Promedio General",
                f"${precio_promedio:,.0f} CLP"
            )

            fig_est = px.bar(
                df_mes,
                x="mes",
                y="precio",
                color="precio",
                title=f"Precio Promedio Mensual - {producto_sel}",
                labels={
                    "mes": "Mes",
                    "precio": "Precio Promedio (CLP)"
                }
            )

            st.plotly_chart(
                fig_est,
                use_container_width=True
            )

        else:
            st.warning(
                "No existen las columnas necesarias para generar el gráfico."
            )

    # ======================================================
    # NIVEL TÁCTICO
    # ======================================================

    with tab_tac:

        st.header("📊 KPI Táctico: Comparación de Precios por Comuna")

        if "comuna" in df_filtrado.columns and "precio" in df_filtrado.columns:

            df_comuna = (
                df_filtrado
                .groupby("comuna")["precio"]
                .mean()
                .reset_index()
                .sort_values("precio", ascending=False)
            )

            fig_tac = px.bar(
                df_comuna,
                x="comuna",
                y="precio",
                color="precio",
                title=f"Precio Promedio por Comuna - {producto_sel}",
                labels={
                    "comuna": "Comuna",
                    "precio": "Precio Promedio (CLP)"
                }
            )

            st.plotly_chart(
                fig_tac,
                use_container_width=True
            )

            st.dataframe(
                df_comuna,
                use_container_width=True,
                hide_index=True
            )

        else:
            st.warning(
                "No existen las columnas necesarias para generar el gráfico."
            )

    # ======================================================
    # NIVEL OPERACIONAL
    # ======================================================

    with tab_op:

        st.header("⚡ KPI Operacional: Top Variaciones Detectadas")

        if "variacion" in df_filtrado.columns:

            top_var = (
                df_filtrado
                .sort_values(
                    "variacion",
                    ascending=False
                )
                .head(10)
            )

            fig_op = px.bar(
                top_var,
                x="lugar_monitoreo",
                y="variacion",
                color="variacion",
                title="Top 10 Variaciones de Precio",
                labels={
                    "lugar_monitoreo": "Lugar de Monitoreo",
                    "variacion": "Variación (%)"
                }
            )

            st.plotly_chart(
                fig_op,
                use_container_width=True
            )

            columnas = [
                "fecha",
                "comuna",
                "lugar_monitoreo",
                "precio",
                "variacion"
            ]

            columnas = [
                c for c in columnas
                if c in top_var.columns
            ]

            st.dataframe(
                top_var[columnas],
                use_container_width=True,
                hide_index=True
            )

        else:
            st.warning(
                "No existe la columna 'variacion' en el dataset."
            )

else:

    st.warning(
        "El archivo CSV está vacío o no se encuentra en esta ruta."
    )