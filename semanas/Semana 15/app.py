import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Configuración de la página web
st.set_page_config(page_title="Dashboard Veterinario Ejecutivo", layout="wide")

st.title(" Cuadro de Mando Integral - Analítica Veterinaria")
st.markdown("---")

# 2. Carga de datos optimizada
@st.cache_data
def cargar_datos():
    # Cambia la ruta a donde guardaste el CSV en el Paso 1
    return pd.read_csv("datos_veterinaria_dashboard.csv")

df = cargar_datos()

# 3. Creación de las Pestañas (Tabs) por Nivel Organizacional
tab_est, tab_tac, tab_op = st.tabs([
    " Nivel Estratégico (CEO)", 
    " Nivel Táctico (Gerente)", 
    " Nivel Operacional (Supervisor)"
])

# ==========================================
# PESTAÑA 1: NIVEL ESTRATÉGICO
# ==========================================
with tab_est:
    st.header(" Concentración del Portafolio por Marca")
    st.caption("Frecuencia: Mensual | Objetivo: Evaluar monopolios de proveedores")
    
    # Procesamiento rápido en Pandas
    total_sku = len(df)
    df_est = df['marca'].value_counts().reset_index()
    df_est.columns = ['marca', 'Cantidad_SKUs']
    df_est['Participacion'] = (df_est['Cantidad_SKUs'] / total_sku) * 100
    
    # Diseño en columnas de Streamlit (Métricas clave arriba)
    col1, col2 = st.columns([1, 2])
    with col1:
        st.metric(label="Total SKUs en Catálogo", value=total_sku)
        st.metric(label="Marca Líder", value=df_est['marca'].iloc[0], delta=f"{df_est['Participacion'].iloc[0]:.1f}% de la góndola")
        st.dataframe(df_est, hide_index=True)
        
    with col2:
        fig, ax = plt.subplots(figsize=(8, 4.5))
        sns.barplot(x="Participacion", y="marca", data=df_est, hue="marca", palette="Blues_r", legend=False, ax=ax)
        sns.despine(left=True, bottom=False)
        ax.set_xlabel("Participación (%)")
        st.pyplot(fig)

# ==========================================
# PESTAÑA 2: NIVEL TÁCTICO
# ==========================================
with tab_tac:
    st.header(" Bandas de Competitividad y Volatilidad de Precios")
    st.caption("Frecuencia: Semanal | Objetivo: Diseñar estrategias de precios y descuentos")
    
    # Filtro interactivo en tiempo real (¡El poder de Streamlit!)
    marcas_seleccionadas = st.multiselect("Filtrar Marcas para Competencia:", options=df['marca'].unique(), default=df['marca'].unique())
    
    df_filtrado = df[df['marca'].isin(marcas_seleccionadas)]
    
    df_tac = df_filtrado.groupby('marca')['precio_kg'].agg(['min', 'mean', 'max']).reset_index().sort_values(by='mean')
    
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.vlines(x=df_tac['marca'], ymin=df_tac['min'], ymax=df_tac['max'], colors='#B0BEC5', alpha=0.7, linewidth=3)
    ax.scatter(df_tac['marca'], df_tac['mean'], color='#1A237E', s=120, zorder=3, label="Promedio")
    ax.scatter(df_tac['marca'], df_tac['min'], color='#2E7D32', marker='^', s=80, zorder=3, label="Mínimo")
    ax.scatter(df_tac['marca'], df_tac['max'], color='#C62828', marker='v', s=80, zorder=3, label="Máximo")
    ax.set_ylabel("Precio por Kg ($)")
    plt.xticks(rotation=25, ha='right')
    ax.legend()
    sns.despine(left=True)
    st.pyplot(fig)

# ==========================================
# PESTAÑA 3: NIVEL OPERACIONAL
# ==========================================
with tab_op:
    st.header(" Matriz de Alertas de Reputación y Calidad")
    st.caption("Frecuencia: Diario / Tiempo Real | Objetivo: Retirar productos defectuosos")
    
    # Control interactivo para el Supervisor (Slider para mover el umbral en vivo)
    umbral_rating = st.slider("Ajustar Umbral Crítico de Rating:", min_value=3.0, max_value=4.5, value=4.0, step=0.1)
    
    df_op = df.copy()
    zona_peligro = df_op[(df_op['rating'] < umbral_rating) & (df_op['opiniones'] > 10)]
    
    st.warning(f" Se han detectado {len(zona_peligro)} SKUs que requieren revisión inmediata en tienda.")
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(df_op["rating"], df_op["opiniones"], alpha=0.4, s=60, color="#78909C")
    ax.scatter(zona_peligro["rating"], zona_peligro["opiniones"], color="#D32F2F", s=110, edgecolor="black", zorder=4)
    ax.axvline(x=umbral_rating, color='#C62828', linestyle='--', alpha=0.6)
    ax.axhline(y=10, color='#C62828', linestyle='--', alpha=0.6)
    
    sns.despine(left=True)
    st.pyplot(fig)
    
    if len(zona_peligro) > 0:
        st.subheader("📋 Lista de Productos para Retiro Inmediato:")
        st.dataframe(zona_peligro[['marca', 'precio_kg', 'rating', 'opiniones']], hide_index=True)