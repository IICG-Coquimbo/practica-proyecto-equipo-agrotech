import streamlit as st
import pandas as pd
import plotly.express as px

# Configuración de página
st.set_page_config(page_title="Dashboard AgroTech", layout="wide", page_icon="🌾")

st.title("🌾 Cuadro de Mando Integral - Analítica AgroTech")
st.markdown("### Monitoreo de Precios Agrícolas, Clima y Macroeconomía")
st.markdown("---")

@st.cache_data
def cargar_datos():
    try:
        df_local = pd.read_csv("datos_agrotech_dashboard.csv")
        df_local.columns = df_local.columns.str.lower()
        if "fecha" in df_local.columns:
            df_local["fecha"] = pd.to_datetime(df_local["fecha"])
        return df_local
    except Exception as e:
        st.error(f"Error cargando CSV: {e}")
        return pd.DataFrame()

df = cargar_datos()

if not df.empty:
    st.sidebar.header("Filtros del Ecosistema")
    
    if "producto" in df.columns:
        df = df[df["producto"].notna()]
        productos_disponibles = sorted(df["producto"].unique().tolist())
    else:
        productos_disponibles = ["Sin Datos"]
        
    producto_sel = st.sidebar.selectbox("Selecciona un Producto:", productos_disponibles)
    df_filtrado = df[df["producto"] == producto_sel].sort_values(by="fecha") if "producto" in df.columns else df

    tab_est, tab_tac, tab_op = st.tabs(["📈 Nivel Estratégico", "📊 Nivel Táctico", "⚡ Nivel Operacional"])

    # 📈 ESTRATÉGICO
    with tab_est:
        st.header("📈 KPI Estratégico: Relación Macroeconómica")
        precio_medio = df_filtrado["precio"].mean() if "precio" in df_filtrado.columns else 0
        tasa_medio = df_filtrado["tasa_cambio"].mean() if "tasa_cambio" in df_filtrado.columns else 0
        
        c1, c2 = st.columns(2)
        c1.metric(label=f"Precio Promedio ({producto_sel})", value=f"${precio_medio:,.0f} CLP")
        c2.metric(label="Tipo de Cambio Promedio (USD/CLP)", value=f"${tasa_medio:,.2f} CLP")
        
        if "fecha" in df_filtrado.columns and "precio" in df_filtrado.columns:
            fig_est = px.area(df_filtrado, x="fecha", y="precio", title=f"Evolución Histórica - {producto_sel}", template="plotly_white")
            fig_est.update_traces(line_color="#2E7D32", fillcolor="#C8E6C9")
            st.plotly_chart(fig_est, use_container_width=True)

    # 📊 TÁCTICO
    with tab_tac:
        st.header("📊 KPI Táctico: Clima vs Precios (Densidad)")
        if "temperatura" in df_filtrado.columns and "precio" in df_filtrado.columns:
            fig_tac = px.density_heatmap(df_filtrado, x="temperatura", y="precio", template="plotly_white", color_continuous_scale="Viridis")
            st.plotly_chart(fig_tac, use_container_width=True)

    # ⚡ OPERACIONAL
    with tab_op:
        st.header(f"⚡ KPI Operacional: Alertas de Volatilidad")
        col_var = "variacion" if "variacion" in df_filtrado.columns else ("variación" if "variación" in df_filtrado.columns else None)
        
        if col_var:
            # Aseguramos que la columna sea numérica para la barra
            df_op = df_filtrado.copy()
            df_op[col_var] = pd.to_numeric(df_op[col_var].replace('%', '', regex=True), errors='coerce').fillna(0)
            
            columnas_a_mostrar = ["año", "calidad", "comuna", "lugar_monitoreo", "mes", "precio", "producto", col_var]
            df_final = df_op[columnas_a_mostrar]
            
            # Usamos column_config para renderizar barras de progreso directamente en st.dataframe
            st.dataframe(
                df_final,
                use_container_width=True,
                hide_index=True,
                column_config={
                    col_var: st.column_config.ProgressColumn(
                        "Variación",
                        help="Variación porcentual de precio",
                        format="%.2f%%",
                        min_value=df_op[col_var].min(),
                        max_value=df_op[col_var].max(),
                    )
                }
            )
        else:
            st.error("No se encontró la columna de variación.")
else:
    st.warning("El archivo CSV no se encuentra o está vacío.")