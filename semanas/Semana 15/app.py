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
        
        # Estandarizar nombres de columnas a minúsculas para evitar conflictos de tipeo
        df_local.columns = df_local.columns.str.lower()
        
        # Asegurar existencia de columna 'fecha'
        if "fecha" in df_local.columns:
            df_local["fecha"] = pd.to_datetime(df_local["fecha"])
        return df_local
    except Exception as e:
        st.error(f"Error cargando CSV: {e}")
        return pd.DataFrame()

df = cargar_datos()

if not df.empty:
    st.sidebar.header("Filtros del Ecosistema")
    
    # Manejo de selector de producto
    if "producto" in df.columns:
        df = df[df["producto"].notna()]
        productos_disponibles = sorted(df["producto"].unique().tolist())
    else:
        productos_disponibles = ["Sin Datos"]
        
    producto_sel = st.sidebar.selectbox("Selecciona un Producto:", productos_disponibles)
    
    # Filtrado por producto seleccionado
    df_filtrado = df[df["producto"] == producto_sel].sort_values(by="fecha") if "producto" in df.columns else df

    tab_est, tab_tac, tab_op = st.tabs(["📈 Nivel Estratégico", "📊 Nivel Táctico", "⚡ Nivel Operacional"])

    # 📈 ESTRATÉGICO (GRÁFICO INTERACTIVO)
    with tab_est:
        st.header("📈 KPI Estratégico: Relación Macroeconómica")
        precio_medio = df_filtrado["precio"].mean() if "precio" in df_filtrado.columns else 0
        tasa_medio = df_filtrado["tasa_cambio"].mean() if "tasa_cambio" in df_filtrado.columns else 0
        
        c1, c2 = st.columns(2)
        c1.metric(label=f"Precio Promedio ({producto_sel})", value=f"${precio_medio:,.0f} CLP")
        c2.metric(label="Tipo de Cambio Promedio (USD/CLP)", value=f"${tasa_medio:,.2f} CLP")
        
        if "fecha" in df_filtrado.columns and "precio" in df_filtrado.columns:
            # Gráfico interactivo de línea con Plotly
            fig_est = px.line(
                df_filtrado, 
                x="fecha", 
                y="precio", 
                title=f"Evolución Histórica del Precio - {producto_sel}",
                labels={"fecha": "Fecha", "precio": "Precio (CLP)"},
                template="plotly_white"
            )
            fig_est.update_traces(line_color="#2E7D32", line_width=2.5)
            st.plotly_chart(fig_est, use_container_width=True)
        else:
            st.warning("Faltan columnas de fecha o precio para este gráfico.")

    # 📊 TÁCTICO (GRÁFICO INTERACTIVO)
    with tab_tac:
        st.header("📊 KPI Táctico: Clima vs Precios")
        
        x_col = "temperatura" if "temperatura" in df_filtrado.columns else (df_filtrado.columns[0] if len(df_filtrado.columns) > 0 else None)
        y_col = "precio" if "precio" in df_filtrado.columns else (df_filtrado.columns[0] if len(df_filtrado.columns) > 0 else None)
        color_col = "comuna" if "comuna" in df_filtrado.columns else None
        
        if x_col and y_col:
            # Gráfico interactivo de dispersión con Plotly
            fig_tac = px.scatter(
                df_filtrado,
                x=x_col,
                y=y_col,
                color=color_col,
                title=f"Análisis de Dispersión: {x_col.title()} vs {y_col.title()}",
                labels={x_col: x_col.title(), y_col: y_col.title()},
                template="plotly_white",
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig_tac.update_traces(marker=dict(size=10, opacity=0.8, line=dict(width=1, color='DarkSlateGrey')))
            st.plotly_chart(fig_tac, use_container_width=True)
        else:
            st.warning("No hay suficientes columnas numéricas disponibles para generar el gráfico táctico.")

# ⚡ OPERACIONAL (COLUMNAS SOLICITADAS + ESTILO COLOR ROJO)
    with tab_op:
        st.header(f"⚡ KPI Operacional: Alertas de Volatilidad para {producto_sel}")
        
        # Mapear nombre por si viene con tilde o sin tilde en el dataset original
        col_var = "variación" if "variación" in df.columns else ("variacion" if "variacion" in df.columns else None)
        
        if col_var:
            # OPTIMIZACIÓN: Obtener mínimos y máximos basados en el producto filtrado
            if not df_filtrado.empty and df_filtrado[col_var].notna().any():
                min_val = float(df_filtrado[col_var].min())
                max_val = float(df_filtrado[col_var].max())
            else:
                min_val, max_val = 0.0, 0.0
            
            # Controlar que si el min y max son iguales (ej. un solo dato), el slider no falle
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
                # Filtro del dataframe basado en el producto Y el slider de variación
                df_filtrado_op = df_filtrado[(df_filtrado[col_var] >= umbral[0]) & (df_filtrado[col_var] <= umbral[1])]
        else:
            st.error("No se encontró la columna de variación en los datos.")
            df_filtrado_op = df_filtrado

        # Columnas solicitadas explícitamente
        columnas_finales = ["año", "calidad", "comuna", "lugar_monitoreo", "mes", "precio", "producto"]
        if col_var:
            columnas_finales.append(col_var)
            
        # Validar cuáles columnas realmente existen en el archivo cargado para evitar KeyError
        columnas_existentes = [c for c in columnas_finales if c in df_filtrado_op.columns]
        df_operacional = df_filtrado_op[columnas_existentes]
        
        # Función para pintar de rojo las variaciones negativas
        def colorear_negativos(val):
            try:
                color = 'red' if float(val) < 0 else 'black'
                return f'color: {color}'
            except:
                return 'color: black'

        # Aplicar el estilo condicional a la columna de variación si existe
        if col_var and col_var in df_operacional.columns:
            df_estilado = df_operacional.style.applymap(
                colorear_negativos, subset=[col_var]
            ).format(subset=[col_var], formatter="{:.2f}%")
        else:
            df_estilado = df_operacional

        # Desplegar tabla interactiva limpia con las columnas ordenadas y filtradas
        st.dataframe(df_estilado, use_container_width=True, hide_index=True)
else:
    st.warning("El archivo CSV está vacío o no se encuentra en esta ruta.")