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

    # 📈 ESTRATÉGICO (GRÁFICO DE ÁREA)
    with tab_est:
        st.header("📈 KPI Estratégico: Relación Macroeconómica")
        precio_medio = df_filtrado["precio"].mean() if "precio" in df_filtrado.columns else 0
        tasa_medio = df_filtrado["tasa_cambio"].mean() if "tasa_cambio" in df_filtrado.columns else 0

        c1, c2 = st.columns(2)
        c1.metric(label=f"Precio Promedio ({producto_sel})", value=f"${precio_medio:,.0f} CLP")
        c2.metric(label="Tipo de Cambio Promedio (USD/CLP)", value=f"${tasa_medio:,.2f} CLP")

        if "fecha" in df_filtrado.columns and "precio" in df_filtrado.columns:
            fig_est = px.area(
                df_filtrado,
                x="fecha",
                y="precio",
                title=f"Evolución Histórica del Precio - {producto_sel}",
                labels={"fecha": "Fecha", "precio": "Precio (CLP)"},
                template="plotly_white",
                color_discrete_sequence=["#1B5E20"]
            )
            fig_est.update_traces(
                line_color="#2E7D32",
                line_width=2,
                fillcolor="rgba(46, 125, 50, 0.3)"
            )
            fig_est.update_layout(
                hovermode="x unified",
                xaxis_title="Fecha",
                yaxis_title="Precio (CLP)",
            )
            st.plotly_chart(fig_est, use_container_width=True)
        else:
            st.warning("Faltan columnas de fecha o precio para este gráfico.")

    # 📊 TÁCTICO (HEATMAP DE CORRELACIÓN)
    with tab_tac:
        st.header("📊 KPI Táctico: Correlación entre Variables")

        columnas_numericas = ["precio", "temperatura", "tasa_cambio", "variacion", "variación"]
        columnas_disponibles = [c for c in columnas_numericas if c in df_filtrado.columns]

        if len(columnas_disponibles) >= 2:
            df_corr = df_filtrado[columnas_disponibles].dropna()
            matriz_corr = df_corr.corr()

            fig_tac = px.imshow(
                matriz_corr,
                text_auto=".2f",
                color_continuous_scale="RdYlGn",
                zmin=-1, zmax=1,
                title=f"Mapa de Correlación entre Variables - {producto_sel}",
                labels=dict(color="Correlación"),
                template="plotly_white"
            )
            fig_tac.update_layout(
                coloraxis_colorbar=dict(
                    title="Correlación",
                    tickvals=[-1, -0.5, 0, 0.5, 1],
                    ticktext=["−1 (inversa)", "−0.5", "0", "0.5", "1 (directa)"]
                )
            )
            st.plotly_chart(fig_tac, use_container_width=True)

            st.markdown("#### 🔍 Interpretación")
            for i in range(len(matriz_corr.columns)):
                for j in range(i + 1, len(matriz_corr.columns)):
                    col_a = matriz_corr.columns[i]
                    col_b = matriz_corr.columns[j]
                    val = matriz_corr.loc[col_a, col_b]
                    if abs(val) >= 0.7:
                        tipo = "fuerte positiva" if val > 0 else "fuerte negativa"
                        st.write(f"- **{col_a}** y **{col_b}**: correlación {tipo} ({val:.2f})")
                    elif abs(val) >= 0.4:
                        tipo = "moderada positiva" if val > 0 else "moderada negativa"
                        st.write(f"- **{col_a}** y **{col_b}**: correlación {tipo} ({val:.2f})")
        else:
            st.warning("No hay suficientes columnas numéricas para generar el heatmap.")

    # ⚡ OPERACIONAL
    with tab_op:
        st.header(f"⚡ KPI Operacional: Alertas de Volatilidad para {producto_sel}")

        col_var = "variación" if "variación" in df.columns else ("variacion" if "variacion" in df.columns else None)

        if col_var:
            if not df_filtrado.empty and df_filtrado[col_var].notna().any():
                min_val = float(df_filtrado[col_var].min())
                max_val = float(df_filtrado[col_var].max())
            else:
                min_val, max_val = 0.0, 0.0

            if min_val == max_val:
                st.info(f"Variación constante para este producto: {min_val:.2f}%")
                df_filtrado_op = df_filtrado
            else:
                umbral = st.slider(
                    "Filtrar por rango de Variación (%):",
                    min_value=min_val,
                    max_value=max_val,
                    value=(min_val, max_val)
                )
                df_filtrado_op = df_filtrado[(df_filtrado[col_var] >= umbral[0]) & (df_filtrado[col_var] <= umbral[1])]
        else:
            st.error("No se encontró la columna de variación en los datos.")
            df_filtrado_op = df_filtrado

        columnas_finales = ["año", "calidad", "comuna", "lugar_monitoreo", "mes", "precio", "producto"]
        if col_var:
            columnas_finales.append(col_var)

        columnas_existentes = [c for c in columnas_finales if c in df_filtrado_op.columns]
        df_operacional = df_filtrado_op[columnas_existentes]

        def colorear_negativos(val):
            try:
                color = 'red' if float(val) < 0 else 'black'
                return f'color: {color}'
            except:
                return 'color: black'

        if col_var and col_var in df_operacional.columns:
            df_estilado = df_operacional.style.applymap(
                colorear_negativos, subset=[col_var]
            ).format(subset=[col_var], formatter="{:.2f}%")
        else:
            df_estilado = df_operacional

        st.dataframe(df_estilado, use_container_width=True, hide_index=True)

else:
    st.warning("El archivo CSV está vacío o no se encuentra en esta ruta.")