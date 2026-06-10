import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# ─────────────────────────────────────────────
# CONFIGURACIÓN DE PÁGINA
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard AgroTech · KPIs",
    layout="wide",
    page_icon="🌿",
    initial_sidebar_state="expanded"
)

# Configuración global de estilo para Matplotlib/Seaborn
sns.set_theme(style="whitegrid")
plt.rcParams['figure.facecolor'] = '#F5F5F0'  # Coincide con GRIS_SUAVE
plt.rcParams['axes.facecolor'] = '#FFFFFF'    # Coincide con BLANCO

# ─────────────────────────────────────────────
# PALETA Y ESTILOS GLOBALES
# ─────────────────────────────────────────────
VERDE_OSCURO  = "#2D5016"
VERDE_MEDIO   = "#4A7C2F"
VERDE_CLARO   = "#8BB04E"
TIERRA        = "#A0522D"
OCRE          = "#C8A84B"
GRIS_SUAVE    = "#F5F5F0"
BLANCO        = "#FFFFFF"
NEGRO         = "#000000"

PALETA_COMUNAS  = ["#4A7C2F", "#C8A84B", "#A0522D"]
PALETA_CLIMA    = {"Temperatura (°C)": "#E07B39", "Humedad (%)": "#4A7C2F", "Radiación UV": "#C8A84B"}

st.markdown(f"""
<style>
    /* Fondo general */
    .stApp {{ background-color: {GRIS_SUAVE}; }}

    /* Sidebar */
    [data-testid="stSidebar"] {{
        background-color: {VERDE_OSCURO};
    }}
    [data-testid="stSidebar"] * {{
        color: {BLANCO} !important;
    }}
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stMultiSelect label,
    [data-testid="stSidebar"] .stSlider label {{
        color: {OCRE} !important;
        font-weight: 600;
        font-size: 0.85rem;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }}

    /* Título principal */
    h1 {{ color: {VERDE_OSCURO} !important; font-weight: 800; }}
    h2 {{ color: {VERDE_MEDIO} !important; }}
    h3 {{ color: {VERDE_OSCURO} !important; }}

    /* Tarjetas KPI personalizadas */
    .kpi-card {{
        background: {BLANCO};
        border-left: 5px solid {VERDE_MEDIO};
        border-radius: 8px;
        padding: 18px 22px;
        margin-bottom: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.07);
    }}
    .kpi-card .label {{
        font-size: 0.78rem;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 4px;
    }}
    .kpi-card .value {{
        font-size: 1.7rem;
        font-weight: 800;
        color: {VERDE_OSCURO};
        line-height: 1.1;
    }}
    .kpi-card .sub {{
        font-size: 0.82rem;
        color: #888;
        margin-top: 4px;
    }}
    .kpi-card.acento {{ border-left-color: {OCRE}; }}
    .kpi-card.alerta {{ border-left-color: {TIERRA}; }}

    /* Tabs */
    [data-baseweb="tab-list"] {{
        gap: 6px;
        background: transparent;
    }}
    [data-baseweb="tab"] {{
        background: {BLANCO};
        border-radius: 8px 8px 0 0 !important;
        padding: 10px 20px !important;
        font-weight: 600;
        color: {VERDE_OSCURO} !important;
        border: 1px solid #ddd;
    }}
    [aria-selected="true"] {{
        background: {VERDE_OSCURO} !important;
        color: {BLANCO} !important;
        border-color: {VERDE_OSCURO} !important;
    }}

    /* Divisor decorativo */
    .divider {{
        height: 3px;
        background: linear-gradient(90deg, {VERDE_OSCURO}, {OCRE}, transparent);
        border-radius: 2px;
        margin: 8px 0 20px 0;
    }}

    /* Badge de nivel */
    .badge {{
        display: inline-block;
        padding: 3px 12px;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 12px;
    }}
    .badge-est  {{ background: {VERDE_OSCURO}; color: white; }}
    .badge-tac  {{ background: {OCRE};         color: {VERDE_OSCURO}; }}
    .badge-op   {{ background: {TIERRA};        color: white; }}

    /* Corregir el texto del expander cuando no se pasa el mouse */
    [data-testid="stExpander"] details summary p {{
        color: #2D5016 !important; /* Fuerza el color verde oscuro */
        font-weight: 600 !important;
    }}

    /* Asegurar que el icono de la flecha del expander también sea visible */
    [data-testid="stExpander"] details summary svg {{
        fill: #2D5016 !important;
        color: #2D5016 !important;
    }}

    /* Tabla */
    .resumen-table {{ font-size: 0.9rem; }}

    /* Corregir las etiquetas de los componentes de control (Radio y Checkbox) */
    [data-testid="stMainBlockContainer"] .stRadio label,
    [data-testid="stMainBlockContainer"] .stCheckbox label {{
        color: {VERDE_OSCURO} !important;
        font-weight: 500 !important;
    }}

    /* Corregir los textos específicos de las opciones dentro del radio horizontal */
    [data-testid="stMainBlockContainer"] [data-testid="stWidgetLabel"] p {{
        color: {VERDE_OSCURO} !important;
    }}
    
    [data-testid="stMainBlockContainer"] [data-baseweb="radio"] div {{
        color: {VERDE_OSCURO} !important;
    }}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# CARGA DE DATOS
# ─────────────────────────────────────────────
@st.cache_data
def cargar_datos():
    df = pd.read_csv("datos_agrotech_dashboard.csv")
    df.columns = df.columns.str.lower()
    df["fecha"] = pd.to_datetime(df["fecha"])
    df["año"] = df["año"].astype(int)
    return df

df_raw = cargar_datos()

PRODUCTOS_AG = [p for p in df_raw["producto"].dropna().unique() if p.lower() != "petroleo"]


# ─────────────────────────────────────────────
# SIDEBAR — FILTROS GLOBALES
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🌿 AgroTech")
    st.markdown("**Dashboard de KPIs**")
    st.markdown("---")

    st.markdown("### Filtros globales")

    productos_sel = st.multiselect(
        "Productos",
        options=sorted(PRODUCTOS_AG),
        default=sorted(PRODUCTOS_AG),
        help="Aplica a todas las pestañas"
    )

    años_disponibles = sorted(df_raw["año"].unique().tolist())
    rango_años = st.slider(
        "Rango de años",
        min_value=int(min(años_disponibles)),
        max_value=int(max(años_disponibles)),
        value=(2023, int(max(años_disponibles))),
        step=1
    )

    comunas_disp = sorted(df_raw["comuna"].dropna().unique().tolist())
    comunas_sel = st.multiselect(
        "Comunas",
        options=comunas_disp,
        default=comunas_disp
    )

    st.markdown("---")
    st.markdown(
        "<small style='color:#000000;'>Datos: Monitoreo agrícola<br>Coquimbo · La Serena · Ovalle</small>",
        unsafe_allow_html=True
    )

# Aplicar filtros globales
df = df_raw[
    (df_raw["producto"].isin(productos_sel)) &
    (df_raw["año"] >= rango_años[0]) &
    (df_raw["año"] <= rango_años[1]) &
    (df_raw["comuna"].isin(comunas_sel))
].copy()

df_agri = df[df["producto"].str.lower() != "petroleo"].copy()


# ─────────────────────────────────────────────
# ENCABEZADO
# ─────────────────────────────────────────────
st.markdown("# 🌿 Cuadro de Mando — AgroTech")
st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

# Envolvemos todo el texto en un span con el color que desees (por ejemplo, el VERDE_OSCURO que ya tienes definido)
st.markdown(
    f"""<span style="color: {VERDE_OSCURO}; font-size: 1.05rem;">
    Mostrando {len(df_agri):,} registros <br>
    Productos: {', '.join(productos_sel) if productos_sel else '—'}· <br>
    Años: {rango_años[0]}–{rango_años[1]}
    </span>""", 
    unsafe_allow_html=True # ¡Importante añadir esto para que reconozca el HTML!
)
st.markdown("")

if df_agri.empty:
    st.warning("⚠️ No hay datos para los filtros seleccionados. Ajusta los filtros del panel lateral.")
    st.stop()


# ═══════════════════════════════════════════════════════════════
# PESTAÑAS
# ═══════════════════════════════════════════════════════════════
tab_est, tab_tac, tab_op = st.tabs([
    "📈  Estratégico",
    "📊  Táctico",
    "⚡  Operativo"
])


# ───────────────────────────────────────────────────────────────
# PESTAÑA 1 · ESTRATÉGICO
# KPI: Precio promedio por punto de venta
# ───────────────────────────────────────────────────────────────
with tab_est:
    st.markdown('<span class="badge badge-est">Nivel Estratégico</span>', unsafe_allow_html=True)
    st.markdown("## Precio promedio por punto de venta")
    st.markdown(f'<span style="color: {VERDE_OSCURO}; font-size: 1.05rem;">Comparación de precios entre Feria Libre y Supermercado por producto agrícola.</span>', unsafe_allow_html=True)
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    df_est = df_agri[df_agri["lugar_monitoreo"].notna()].copy()

    if df_est.empty:
        st.warning("Sin datos de lugar de monitoreo para los filtros actuales.")
    else:
        # ── KPI Cards ──────────────────────────────────────────
        prom_feria = df_est[df_est["lugar_monitoreo"] == "Feria libre"]["precio"].mean()
        prom_super = df_est[df_est["lugar_monitoreo"] == "Supermercado"]["precio"].mean()
        diferencia = ((prom_super - prom_feria) / prom_feria * 100) if prom_feria > 0 else 0
        prod_mayor_diff = (
            df_est.groupby(["producto", "lugar_monitoreo"])["precio"]
            .mean().unstack(fill_value=0)
        )
        if "Feria libre" in prod_mayor_diff.columns and "Supermercado" in prod_mayor_diff.columns:
            prod_mayor_diff["diff_pct"] = (
                (prod_mayor_diff["Supermercado"] - prod_mayor_diff["Feria libre"])
                / prod_mayor_diff["Feria libre"] * 100
            )
            top_prod = prod_mayor_diff["diff_pct"].idxmax()
            top_diff = prod_mayor_diff["diff_pct"].max()
        else:
            top_prod, top_diff = "—", 0

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="label">Precio Prom. Feria Libre</div>
                <div class="value">${prom_feria:,.0f}</div>
                <div class="sub">CLP por unidad</div>
            </div>""", unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
            <div class="kpi-card acento">
                <div class="label">Precio Prom. Supermercado</div>
                <div class="value">${prom_super:,.0f}</div>
                <div class="sub">CLP por unidad</div>
            </div>""", unsafe_allow_html=True)
        with c3:
            st.markdown(f"""
            <div class="kpi-card alerta">
                <div class="label">Brecha Canal</div>
                <div class="value">+{diferencia:.1f}%</div>
                <div class="sub">Supermercado vs Feria</div>
            </div>""", unsafe_allow_html=True)
        with c4:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="label">Mayor diferencia</div>
                <div class="value">{top_prod}</div>
                <div class="sub">+{top_diff:.1f}% en supermercado</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("")

        # ── Gráfico barras agrupadas (Seaborn) ──────────────────
        df_bar = (
            df_est.groupby(["producto", "lugar_monitoreo"])["precio"]
            .mean().reset_index()
            .rename(columns={"precio": "Precio Promedio (CLP)", "lugar_monitoreo": "Canal"})
        )

        fig_bar, ax_bar = plt.subplots(figsize=(10, 5))
        sns.barplot(
            data=df_bar,
            x="producto",
            y="Precio Promedio (CLP)",
            hue="Canal",
            palette={"Feria libre": VERDE_MEDIO, "Supermercado": OCRE},
            ax=ax_bar
        )
        
        # Añadir etiquetas de texto sobre las barras
        for container in ax_bar.containers:
            ax_bar.bar_label(container, fmt='$%:,.0f', padding=3, fontsize=9)

        ax_bar.set_title("Precio promedio por producto y canal de venta", fontsize=14, pad=15, weight='bold', color=VERDE_OSCURO)
        ax_bar.set_xlabel("Producto", fontsize=11)
        ax_bar.set_ylabel("Precio promedio (CLP)", fontsize=11)
        ax_bar.legend(title="Canal de venta")
        sns.despine()
        
        st.pyplot(fig_bar)

        # ── Heatmap (Seaborn) ──────────────────────────────────
        st.markdown(f'<div style="color: {VERDE_OSCURO}; font-size: 1.2rem; font-weight: bold; margin-top: 20px; margin-bottom: 10px;">Mapa de calor — Precio promedio por producto y canal</div>', unsafe_allow_html=True)
        df_heat = (
            df_est.groupby(["producto", "lugar_monitoreo"])["precio"]
            .mean().unstack(fill_value=0).round(0)
        )

        fig_heat, ax_heat = plt.subplots(figsize=(8, 4))
        
        # Crear un colormap personalizado basado en tu paleta verde
        cmap_verdes = sns.light_palette(VERDE_OSCURO, as_cmap=True)
        
        sns.heatmap(
            df_heat,
            annot=True,
            fmt=",.0f",
            cmap=cmap_verdes,
            linewidths=.5,
            cbar_kws={'label': 'CLP'},
            ax=ax_heat,
            annot_kws={"size": 11, "weight": "bold"}
        )
        
        ax_heat.set_title("Precio promedio (CLP) — Heatmap canal × producto", fontsize=12, pad=15, weight='bold', color=VERDE_OSCURO)
        ax_heat.set_xlabel("Canal de venta", fontsize=10)
        ax_heat.set_ylabel("Producto", fontsize=10)
        
        st.pyplot(fig_heat)


# ───────────────────────────────────────────────────────────────
# PESTAÑA 2 · TÁCTICO
# KPI: Comparativa de precios por comuna (Boxplot)
# ───────────────────────────────────────────────────────────────
with tab_tac:
    st.markdown('<span class="badge badge-tac">Nivel Táctico</span>', unsafe_allow_html=True)
    st.markdown("## Comparativa de precios por comuna")
    st.markdown(f'<span style="color: {VERDE_OSCURO}; font-size: 1.05rem;">Distribución de precios agrícolas según localidad, con análisis de dispersión y valores atípicos.</span>', unsafe_allow_html=True)
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    df_tac = df_agri[df_agri["comuna"].notna()].copy()

    if df_tac.empty:
        st.warning("Sin datos de comuna para los filtros actuales.")
    else:
        col_ctrl1, col_ctrl2 = st.columns([2, 1])
        with col_ctrl1:
            agrupacion = st.radio(
                "Agrupar boxplot por:",
                options=["Comuna", "Lugar de monitoreo"],
                horizontal=True
            )
        with col_ctrl2:
            mostrar_outliers = st.checkbox("Mostrar valores atípicos", value=True)

        col_map = {"Comuna": "comuna", "Lugar de monitoreo": "lugar_monitoreo"}
        col_color = col_map[agrupacion]

        df_tac_box = df_tac[df_tac[col_color].notna()].copy()
        
        # Configuración de outliers en Matplotlib
        fliersize = 4 if mostrar_outliers else 0

        # ── Boxplot (Seaborn) ──────────────────────────────────
        fig_box, ax_box = plt.subplots(figsize=(10, 5))
        
        # Si la paleta común se queda corta por la cantidad de categorías, Seaborn usa fallback ordenado
        sns.boxplot(
            data=df_tac_box,
            x="producto",
            y="precio",
            hue=col_color,
            palette=PALETA_COMUNAS if len(df_tac_box[col_color].unique()) <= len(PALETA_COMUNAS) else "Set2",
            fliersize=fliersize,
            ax=ax_box
        )
        
        ax_box.set_title(f"Distribución de precios por producto · agrupado por {agrupacion.lower()}", fontsize=13, pad=15, weight='bold', color=VERDE_OSCURO)
        ax_box.set_xlabel("Producto", fontsize=11)
        ax_box.set_ylabel("Precio (CLP)", fontsize=11)
        ax_box.legend(title=agrupacion)
        sns.despine()
        
        st.pyplot(fig_box)

        # ── Tabla resumen ──────────────────────────────────────
        st.markdown(f'<div style="color: {VERDE_OSCURO}; font-size: 1.2rem; font-weight: bold; margin-top: 25px; margin-bottom: 10px;">Resumen estadístico por producto y {agrupacion.lower()}</div>', unsafe_allow_html=True)
        df_resumen = (
            df_tac_box.groupby(["producto", col_color])["precio"]
            .agg(
                Mediana="median",
                Promedio="mean",
                Mínimo="min",
                Máximo="max",
                Registros="count"
            )
            .round(0)
            .reset_index()
            .rename(columns={"producto": "Producto", col_color: agrupacion})
        )
        df_resumen["Mediana"] = df_resumen["Mediana"].apply(lambda x: f"${x:,.0f}")
        df_resumen["Promedio"] = df_resumen["Promedio"].apply(lambda x: f"${x:,.0f}")
        df_resumen["Mínimo"]   = df_resumen["Mínimo"].apply(lambda x: f"${x:,.0f}")
        df_resumen["Máximo"]   = df_resumen["Máximo"].apply(lambda x: f"${x:,.0f}")

        st.dataframe(
            df_resumen,
            use_container_width=True,
            hide_index=True,
            height=280
        )


# ───────────────────────────────────────────────────────────────
# PESTAÑA 3 · OPERATIVO
# KPI: Evolución meteorológica semanal
# ───────────────────────────────────────────────────────────────
with tab_op:
    st.markdown('<span class="badge badge-op">Nivel Operativo</span>', unsafe_allow_html=True)
    st.markdown("## Evolución meteorológica semanal")
    st.markdown(f'<span style="color: {VERDE_OSCURO}; font-size: 1.05rem;">Comportamiento de variables climáticas semana a semana en la zona de monitoreo.</span>', unsafe_allow_html=True)
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # ── Controles locales ──────────────────────────────────────
    col_op1, col_op2 = st.columns([2, 1])
    with col_op1:
        variables_sel = st.multiselect(
            "Variables climáticas a visualizar:",
            options=["Temperatura (°C)", "Humedad (%)", "Radiación UV"],
            default=["Temperatura (°C)", "Humedad (%)", "Radiación UV"],
            help="Selecciona una o más variables"
        )
    with col_op2:
        semanas_rango = st.slider(
            "Semanas del año:",
            min_value=1,
            max_value=53,
            value=(1, 52),
            step=1
        )

    map_cols = {
        "Temperatura (°C)": "temperatura",
        "Humedad (%)":       "humedad",
        "Radiación UV":      "radiacion_uv"
    }

    # Agregar por semana (promedio de todos los años en rango)
    df_clima = (
        df_raw[
            (df_raw["semana_año"] >= semanas_rango[0]) &
            (df_raw["semana_año"] <= semanas_rango[1])
        ]
        .groupby("semana_año")[["temperatura", "humedad", "radiacion_uv", "precipitaciones"]]
        .mean()
        .reset_index()
    )

    if df_clima.empty or not variables_sel:
        st.warning("Selecciona al menos una variable climática y un rango de semanas.")
    else:
        # ── Tarjetas resumen clima ─────────────────────────────
        temp_prom = df_clima["temperatura"].mean()
        hum_prom  = df_clima["humedad"].mean()
        uv_max    = df_clima["radiacion_uv"].max()
        semana_uv = int(df_clima.loc[df_clima["radiacion_uv"].idxmax(), "semana_año"])

        cc1, cc2, cc3 = st.columns(3)
        with cc1:
            st.markdown(f"""
            <div class="kpi-card alerta">
                <div class="label">Temperatura promedio del período</div>
                <div class="value">{temp_prom:.1f} °C</div>
                <div class="sub">Semanas {semanas_rango[0]}–{semanas_rango[1]}</div>
            </div>""", unsafe_allow_html=True)
        with cc2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="label">Humedad promedio del período</div>
                <div class="value">{hum_prom:.1f} %</div>
                <div class="sub">Semanas {semanas_rango[0]}–{semanas_rango[1]}</div>
            </div>""", unsafe_allow_html=True)
        with cc3:
            st.markdown(f"""
            <div class="kpi-card acento">
                <div class="label">Peak de Radiación UV</div>
                <div class="value">{uv_max:.1f}</div>
                <div class="sub">Semana {semana_uv} del año</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("")

        # ── Gráfico de líneas múltiples con eje Y dual (Matplotlib) ──
        usa_eje_dual = (
            "Temperatura (°C)" in variables_sel and
            len([v for v in variables_sel if v != "Temperatura (°C)"]) > 0
        )

        fig_clima, ax1 = plt.subplots(figsize=(10, 5))

        lines = []
        labels = []

        if usa_eje_dual:
            ax2 = ax1.twinx()  # Crear el eje Y secundario
            
            if "Temperatura (°C)" in variables_sel:
                line1 = ax1.plot(
                    df_clima["semana_año"], df_clima["temperatura"],
                    color=PALETA_CLIMA["Temperatura (°C)"], linewidth=2.5,
                    marker='o', label="Temperatura (°C)"
                )
                ax1.set_ylabel("Temperatura (°C)", color=PALETA_CLIMA["Temperatura (°C)"])
                ax1.tick_params(axis='y', labelcolor=PALETA_CLIMA["Temperatura (°C)"])
                lines += line1
                labels.append("Temperatura (°C)")

            for var in variables_sel:
                if var == "Temperatura (°C)":
                    continue
                col_data = map_cols[var]
                line2 = ax2.plot(
                    df_clima["semana_año"], df_clima[col_data],
                    color=PALETA_CLIMA[var], linewidth=2.5, linestyle="--",
                    marker='s', label=var
                )
                lines += line2
                labels.append(var)
                
            ax2.set_ylabel("Humedad (%) / Radiación UV", color=VERDE_MEDIO)
            ax2.tick_params(axis='y', labelcolor=VERDE_MEDIO)
            ax2.grid(False) # Evita superposición de grillas

        else:
            # Si no hay eje dual, todo va en el eje primario
            for var in variables_sel:
                col_data = map_cols[var]
                line = ax1.plot(
                    df_clima["semana_año"], df_clima[col_data],
                    color=PALETA_CLIMA[var], linewidth=2.5,
                    marker='o', label=var
                )
                lines += line
                labels.append(var)
            ax1.set_ylabel("Valor")

        # Área sombreada del rango seleccionado (axvspan en matplotlib)
        ax1.axvspan(semanas_rango[0], semanas_rango[1], color=VERDE_CLARO, alpha=0.07, label="Rango seleccionado")
        
        # Configurar títulos e interacciones comunes
        ax1.set_title("Evolución de variables climáticas por semana del año", fontsize=13, pad=15, weight='bold', color=VERDE_OSCURO)
        ax1.set_xlabel("Semana del año")
        ax1.set_xticks(np.arange(min(df_clima["semana_año"]), max(df_clima["semana_año"])+1, 4))
        
        # Leyenda unificada para ambos ejes
        ax1.legend(lines, labels, loc='upper right')
        
        st.pyplot(fig_clima)

        # ── Tabla de datos semanales ───────────────────────────
        with st.expander("📋 Ver tabla de datos semanales"):
            cols_mostrar = ["semana_año"] + [map_cols[v] for v in variables_sel]
            rename_map = {
                "semana_año":    "Semana",
                "temperatura":   "Temperatura (°C)",
                "humedad":        "Humedad (%)",
                "radiacion_uv":  "Radiación UV",
                "precipitaciones": "Precipitaciones"
            }
            df_tabla_clima = (
                df_clima[cols_mostrar]
                .rename(columns=rename_map)
                .round(2)
            )
            st.dataframe(df_tabla_clima, use_container_width=True, hide_index=True)
