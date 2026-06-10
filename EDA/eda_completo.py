import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import silhouette_score
import warnings
warnings.filterwarnings("ignore")

print("\n" + "=" * 80)
print("PASO 1: CONECTAR A MONGODB Y CARGAR DATOS")
print("=" * 80)

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("DB_NAME")]
processed = db[os.getenv("PROCESSED_COLLECTION")]

df = pd.DataFrame(list(processed.find({})))
print(f"\n✅ Conectado a MongoDB con {len(df):,} registros cargados.")

print("\n" + "=" * 80)
print("PASO 2: DATA WRANGLING - LIMPIEZA Y VALIDACIÓN")
print("=" * 80)

df = df[df["producto"].notna()].drop_duplicates()
print(f"✅ Filtrado y eliminación de duplicados completada. Registros finales: {len(df):,}")

# ── Asegurar tipos numéricos (el processor ya los normaliza, pero por si hay
#    documentos legacy que pasen directo a processed sin pasar por processor_v4)
for col_num in ["precio", "precio_normalizado", "maximo", "minimo", "p_apertura",
                "tasa_cambio", "temperatura", "humedad", "precipitaciones",
                "radiacion_uv", "mes_num", "estacionalidad_index"]:
    if col_num in df.columns:
        df[col_num] = pd.to_numeric(df[col_num], errors="coerce")

# ── Columna de trabajo: precio_normalizado preferido, fallback a precio
if "precio_normalizado" not in df.columns:
    df["precio_normalizado"] = pd.to_numeric(df["precio"], errors="coerce")

# ── Columna para análisis de mercado de divisas: p_apertura (nuevo) / apertura (legacy)
col_apertura = "p_apertura" if "p_apertura" in df.columns else None

# ═══════════════════════════════════════════════════════════════════════════════
# RESTRUCTURACIÓN DEL PETRÓLEO COMO VARIABLE EXPLICATIVA DE CONTEXTO
# ═══════════════════════════════════════════════════════════════════════════════

# 1. Separar el petróleo para calcular su valor promedio real por mes numérico
df_petroleo_base = df[df["producto"].str.lower() == "petroleo"]
petroleo_mensual_map = df_petroleo_base.groupby("mes_num")["precio_normalizado"].mean().to_dict()

# Fallback de seguridad: por si algún mes no tuviera registro, usamos la media global
media_global_petroleo = df_petroleo_base["precio_normalizado"].mean() if not df_petroleo_base.empty else 0.0

# 2. Filtrar el DataFrame principal para que las FILAS contengan SOLO productos agrícolas
# (Reemplaza definitivamente al df_agricola_boxplot temporal)
df = df[df["producto"].str.lower() != "petroleo"].copy()

# 3. Inyectar el precio del petróleo como una nueva VARIABLE EXPLICATIVA VERTICAL
df["precio_petroleo_contexto"] = df["mes_num"].map(petroleo_mensual_map)
df["precio_petroleo_contexto"].fillna(media_global_petroleo, inplace=True)

print(f"   ↳ 🛢️  Petróleo extraído de las filas y mapeado como columna de contexto.")
print(f"   ↳ 🍏 Registros agrícolas puros resultantes para análisis: {len(df):,}")

print("\n" + "=" * 80)
print("PASO 3: ANÁLISIS DESCRIPTIVO - CORRELACIONES Y AGRUPACIONES")
print("=" * 80)

print("std      : Desviación estándar")
print("cv_pct   : Coeficiente de variación — dispersión relativa (%)")
print("iqr      : Rango intercuartílico (Q3 - Q1)")
print("asimetria: Skewness — forma de la distribución")

# ── Formateador de moneda CLP (solo para display, no altera los datos)
def fmt_clp(val):
    """Formatea un número como precio CLP con signo $."""
    try:
        return f"${int(val):,}".replace(",", ".")
    except (ValueError, TypeError):
        return str(val)

COLS_PRECIO = ["media", "mediana", "std", "q1", "q3", "iqr", "minimo", "maximo"]
COLS_PORCENT = ["cv_pct"]
COLS_NEUTRAS = ["n", "asimetria"]

def imprimir_tabla_formateada(df_agg):
    """
    Imprime el DataFrame con formato $CLP en columnas de precio
    y sin alterar los valores numéricos internos.
    """
    # Aplanar MultiIndex si existe
    if isinstance(df_agg.columns, pd.MultiIndex):
        df_display = df_agg.copy()
        df_display.columns = df_agg.columns.get_level_values(1)
    else:
        df_display = df_agg.copy()

    # Aplicar formato por tipo de columna
    for col in df_display.columns:
        if col in COLS_PRECIO:
            df_display[col] = df_display[col].apply(fmt_clp)
        elif col in COLS_PORCENT:
            df_display[col] = df_display[col].apply(
                lambda v: f"{v:.1f}%" if pd.notna(v) else "—"
            )
        elif col == "asimetria":
            df_display[col] = df_display[col].apply(
                lambda v: f"{v:+.3f}" if pd.notna(v) else "—"
            )
    print(df_display.to_string())

# ── Helper: construye el bloque de estadísticas enriquecidas
def describe_enriquecido(df_input, grupo, variable="precio_normalizado"):
    def cv(x):       return round(x.std() / x.mean() * 100, 1) if x.mean() != 0 else np.nan
    def q1(x):       return x.quantile(0.25)
    def q3(x):       return x.quantile(0.75)
    def iqr(x):      return x.quantile(0.75) - x.quantile(0.25)
    def skewness(x): return round(x.skew(), 3)

    agg = df_input.groupby(grupo)[variable].agg(
        n         ="count",
        media     ="mean",
        mediana   ="median",
        std       ="std",
        cv_pct    =cv,
        q1        =q1,
        q3        =q3,
        iqr       =iqr,
        minimo    ="min",
        maximo    ="max",
        asimetria =skewness
    ).round({"media": 0, "mediana": 0, "std": 0,
             "q1": 0, "q3": 0, "iqr": 0,
             "minimo": 0, "maximo": 0})

    agg.columns = pd.MultiIndex.from_tuples(
        [(variable, c) for c in agg.columns]
    )
    return agg

# ── Helper: interpretación automática por tabla
def interpretar_tabla(df_agg, metrica, variable="precio_normalizado"):
    """
    Genera una interpretación textual basada en los valores calculados.
    Opera sobre el DataFrame numérico (antes del formateo).
    """
    if isinstance(df_agg.columns, pd.MultiIndex):
        df = df_agg[variable].copy()
    else:
        df = df_agg.copy()

    lineas = []

    # Dispersión relativa: quién tiene más y menos variabilidad
    if "cv_pct" in df.columns:
        max_cv = df["cv_pct"].idxmax()
        min_cv = df["cv_pct"].idxmin()
        lineas.append(
            f"   • Dispersión: '{max_cv}' presenta la mayor variabilidad relativa "
            f"(CV={df.loc[max_cv,'cv_pct']:.1f}%), mientras que '{min_cv}' "
            f"es el más estable (CV={df.loc[min_cv,'cv_pct']:.1f}%)."
        )

    # Brecha media-mediana: señal de sesgo o outliers
    if "media" in df.columns and "mediana" in df.columns:
        df["_brecha"] = ((df["media"] - df["mediana"]) / df["mediana"] * 100).round(1)
        max_brecha = df["_brecha"].abs().idxmax()
        val_brecha = df.loc[max_brecha, "_brecha"]
        if abs(val_brecha) > 10:
            direccion = "superiores" if val_brecha > 0 else "inferiores"
            lineas.append(
                f"   • Sesgo por outliers: '{max_brecha}' tiene su media un "
                f"{abs(val_brecha):.1f}% {'por encima' if val_brecha > 0 else 'por debajo'} "
                f"de la mediana → valores extremos {direccion} distorsionan la media."
            )
        df.drop(columns=["_brecha"], inplace=True)

    # Rango de precios más amplio
    if "minimo" in df.columns and "maximo" in df.columns:
        df["_rango"] = df["maximo"] - df["minimo"]
        max_rango = df["_rango"].idxmax()
        lineas.append(
            f"   • Rango más amplio: '{max_rango}' "
            f"(${int(df.loc[max_rango,'minimo']):,} – ${int(df.loc[max_rango,'maximo']):,} CLP)."
            .replace(",", ".")
        )
        df.drop(columns=["_rango"], inplace=True)

    # Asimetría: detectar distribuciones no normales
    if "asimetria" in df.columns:
        asim_altos = df[df["asimetria"].abs() > 1]["asimetria"]
        if not asim_altos.empty:
            casos = ", ".join(
                [f"'{idx}' ({v:+.2f})" for idx, v in asim_altos.items()]
            )
            lineas.append(
                f"   • Distribución asimétrica (|skew|>1): {casos} → "
                f"preferir Spearman en correlaciones con estas categorías."
            )
        else:
            lineas.append(
                f"   • Todas las categorías tienen distribución aproximadamente "
                f"simétrica (|skew| ≤ 1)."
            )

    for linea in lineas:
        print(linea)

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Estadísticas por dimensión categórica + interpretación
# ═══════════════════════════════════════════════════════════════════════════════
for metrica in ["producto", "comuna", "lugar_monitoreo", "calidad", "mes_num"]:
    if metrica in df.columns:
        print(f"\n{'─'*80}")
        print(f"  📊  ESTADÍSTICAS POR {metrica.upper()}")
        print(f"{'─'*80}")
        res = describe_enriquecido(df, metrica)
        imprimir_tabla_formateada(res)
        print(f"\n  📝  Interpretación:")
        interpretar_tabla(res, metrica)

# ═══════════════════════════════════════════════════════════════════════════════
# 2. Tasa de cambio USD/CLP
# ═══════════════════════════════════════════════════════════════════════════════
if "tasa_cambio" in df.columns and df["tasa_cambio"].notna().any():
    print(f"\n{'─'*80}")
    print("  📊  TASA DE CAMBIO USD/CLP — ESTADÍSTICAS MENSUALES")
    print(f"{'─'*80}")
    tc = df.groupby("mes_num")["tasa_cambio"].agg(
        n      ="count",
        media  ="mean",
        mediana="median",
        std    ="std",
        cv_pct =lambda x: round(x.std() / x.mean() * 100, 2),
        minimo ="min",
        maximo ="max"
    ).round(2)
    # Formato $CLP solo para columnas de precio
    tc_display = tc.copy()
    for col in ["media", "mediana", "std", "minimo", "maximo"]:
        tc_display[col] = tc_display[col].apply(fmt_clp)
    tc_display["cv_pct"] = tc_display["cv_pct"].apply(lambda v: f"{v:.2f}%")
    print(tc_display.to_string())

    cv_max = (df.groupby("mes_num")["tasa_cambio"]
              .apply(lambda x: x.std() / x.mean() * 100).max())
    umbral = cv_max < 5
    print(f"\n  📝  Interpretación:")
    print(f"   • CV máximo intramensual: {cv_max:.2f}% → "
          f"{'variabilidad baja en todos los meses.' if umbral else 'variabilidad relevante detectada.'}")
    if umbral:
        print(f"   • La tasa de cambio tiene poca dispersión dentro de cada mes.")
        print(f"   • Su utilidad estará en la variación intermensual (12 puntos),")
        print(f"     no en la dispersión individual. Evaluar si aporta poder")
        print(f"     explicativo adicional al modelo o si puede descartarse.")

# ═══════════════════════════════════════════════════════════════════════════════
# 3. Precio del petróleo
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print("  📊  PRECIO DEL PETRÓLEO (CONTEXTO LOGÍSTICO) — ESTADÍSTICAS MENSUALES")
print(f"{'─'*80}")
if "precio_petroleo_contexto" in df.columns:
    pet = df.groupby("mes_num")["precio_petroleo_contexto"].agg(
        n      ="count",
        media  ="mean",
        mediana="median",
        std    ="std",
        minimo ="min",
        maximo ="max"
    ).round(0)
    pet_display = pet.copy()
    for col in ["media", "mediana", "minimo", "maximo"]:
        pet_display[col] = pet_display[col].apply(fmt_clp)
    pet_display["std"] = pet_display["std"].apply(
        lambda v: f"{v:.0f}" if pd.notna(v) else "—"
    )
    print(pet_display.to_string())

    std_unico = (df.groupby("mes_num")["precio_petroleo_contexto"].std() == 0).all()
    if std_unico:
        print(f"\n  📝  Interpretación:")
        print(f"   • std = 0 en todos los meses: valor único por mes (promedio mapeado).")
        print(f"   • No existe dispersión intramensual real → no usar como variable")
        print(f"     de variabilidad, solo como referencia de nivel mensual.")
        print(f"   • Para correlaciones: trabajar con los 12 promedios mensuales.")
        print(f"   • Mes con precio más alto: mes "
              f"{pet['media'].idxmax()} (${int(pet['media'].max()):,} CLP)."
              .replace(",", "."))
        print(f"   • Mes con precio más bajo: mes "
              f"{pet['media'].idxmin()} (${int(pet['media'].min()):,} CLP)."
              .replace(",", "."))
print("\n" + "=" * 80)
print("PASO 4: VISUALIZACIÓN MULTIVARIADA — ANÁLISIS DESCRIPTIVO (6 PRODUCTOS)")
print("=" * 80)

import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import numpy as np  # Asegura la importación para el triu de las matrices
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("darkgrid")

# 1. Expandimos a los 6 productos requeridos por el negocio
PRODUCTOS_INTERES = ["Uva", "Palta", "Naranja", "Limón", "Tomate", "Papa"]
MESES_NOMBRES     = ["Ene","Feb","Mar","Abr","May","Jun",
                     "Jul","Ago","Sep","Oct","Nov","Dic"]

# 2. Extendemos la paleta con colores corporativos/armónicos para Tomate y Papa
PALETA_PRODUCTOS  = {
    "Uva": "#8e44ad",      # Púrpura
    "Palta": "#27ae60",    # Verde
    "Naranja": "#e67e22",  # Naranja
    "Limón": "#f1c40f",    # Amarillo
    "Tomate": "#c0392b",   # Rojo Oscuro
    "Papa": "#8c6239"      # Café/Tierra
}

# ── Formatter seguro: escapa $ para que matplotlib no lo interprete como LaTeX
def fmt_clp_ax(x, _):
    """Formatea eje Y como precio CLP sin triggerear el parser LaTeX de matplotlib."""
    return r"$\$${:,}".format(int(x)).replace(",", ".")

def lbl_clp(valor):
    """Formatea una anotación de texto como precio CLP."""
    return r"$\${:,}".format(int(valor)).replace(",", ".")

# ─────────────────────────────────────────────────────────────────────────────
# FIGURA 1 — Distribuciones de precio (4 paneles)
# ─────────────────────────────────────────────────────────────────────────────
fig1, axes = plt.subplots(2, 2, figsize=(16, 10))
fig1.suptitle("Distribución de Precios Agrícolas — Análisis Descriptivo",
              fontsize=14, fontweight="bold", y=1.01)

# ── Panel 1: Boxplot por producto (ordenado por mediana automáticamente para los 6)
df_interes    = df[df["producto"].isin(PRODUCTOS_INTERES)]
orden_mediana = (df_interes.groupby("producto")["precio_normalizado"]
                 .median().sort_values(ascending=False).index.tolist())

sns.boxplot(
    data=df_interes, x="producto", y="precio_normalizado",
    order=orden_mediana, ax=axes[0, 0],
    palette=[PALETA_PRODUCTOS[p] for p in orden_mediana],
    width=0.55, linewidth=1.2,
    flierprops=dict(marker="o", markersize=3, alpha=0.4)
)
medias = df_interes.groupby("producto")["precio_normalizado"].mean()
for i, prod in enumerate(orden_mediana):
    axes[0, 0].plot(i, medias[prod], marker="D", color="white",
                    markersize=6, zorder=5, label="Media" if i == 0 else "")
axes[0, 0].set_title("Distribución por Producto (mediana + IQR + outliers)\n"
                      "◆ = media  |  distancia a mediana indica sesgo")
axes[0, 0].set_xlabel("")
axes[0, 0].set_ylabel("Precio (CLP)")
axes[0, 0].yaxis.set_major_formatter(mticker.FuncFormatter(fmt_clp_ax))
axes[0, 0].tick_params(axis="x", rotation=15)
axes[0, 0].legend(fontsize=9)

# ── Panel 2: Boxplot por canal de comercialización + CV anotado (usa el df global de forma segura)
sns.boxplot(
    data=df, x="lugar_monitoreo", y="precio_normalizado",
    ax=axes[0, 1], palette="husl", width=0.5, linewidth=1.2,
    flierprops=dict(marker="o", markersize=3, alpha=0.4)
)
canales_unicos = df["lugar_monitoreo"].dropna().unique()
for i, canal in enumerate(canales_unicos):
    sub = df[df["lugar_monitoreo"] == canal]["precio_normalizado"]
    cv  = sub.std() / sub.mean() * 100
    y_pos = sub.quantile(0.95)
    axes[0, 1].text(i, y_pos * 1.01, f"CV={cv:.0f}%",
                    ha="center", fontsize=9, color="#2c3e50", fontweight="bold")
axes[0, 1].set_title("Precio por Canal de Comercialización\n"
                      "CV = coeficiente de variación (dispersión relativa)")
axes[0, 1].set_xlabel("")
axes[0, 1].set_ylabel("Precio (CLP)")
axes[0, 1].yaxis.set_major_formatter(mticker.FuncFormatter(fmt_clp_ax))

# ── Panel 3: Media vs mediana mensual con banda de brecha
precio_mes_media   = df.groupby("mes_num")["precio_normalizado"].mean()
precio_mes_mediana = df.groupby("mes_num")["precio_normalizado"].median()

axes[1, 0].plot(precio_mes_media.index, precio_mes_media.values,
                marker="o", color="#e74c3c", linewidth=2,
                linestyle="--", label="Media", alpha=0.85)
axes[1, 0].plot(precio_mes_mediana.index, precio_mes_mediana.values,
                marker="s", color="#2ecc71", linewidth=2.5, label="Mediana")
axes[1, 0].fill_between(precio_mes_media.index,
                        precio_mes_media.values,
                        precio_mes_mediana.values,
                        alpha=0.13, color="#e74c3c",
                        label="Brecha media-mediana")
axes[1, 0].set_title("Precio Mensual: Media vs Mediana\n"
                      "Brecha = efecto de outliers y asimetría")
axes[1, 0].set_xticks(range(1, 13))
axes[1, 0].set_xticklabels(MESES_NOMBRES, rotation=30)
axes[1, 0].set_ylabel("Precio (CLP)")
axes[1, 0].yaxis.set_major_formatter(mticker.FuncFormatter(fmt_clp_ax))
axes[1, 0].legend(fontsize=9)
axes[1, 0].set_ylim(precio_mes_mediana.min() * 0.88,
                     precio_mes_media.max() * 1.08)
axes[1, 0].grid(True, linestyle="--", alpha=0.5)

# ── Panel 4: Violin por comuna (reemplaza barplot engañoso)
comunas_orden = (df.groupby("comuna")["precio_normalizado"]
                 .median().sort_values(ascending=False).index.tolist())
sns.violinplot(
    data=df, x="comuna", y="precio_normalizado", order=comunas_orden,
    ax=axes[1, 1], palette="pastel", inner="quartile", linewidth=1.2
)
for i, comuna in enumerate(comunas_orden):
    media_c = df[df["comuna"] == comuna]["precio_normalizado"].mean()
    axes[1, 1].plot(i, media_c, marker="D", color="#2c3e50",
                    markersize=7, zorder=5)
    axes[1, 1].text(i, media_c + 180, lbl_clp(media_c),
                    ha="center", fontsize=8.5,
                    color="#2c3e50", fontweight="bold")
axes[1, 1].set_title("Distribución por Comuna\n"
                      "◆ = media  |  líneas = Q1, mediana, Q3\n"
                      "(diferencia intercomunal mínima)")
axes[1, 1].set_xlabel("")
axes[1, 1].set_ylabel("Precio (CLP)")
axes[1, 1].yaxis.set_major_formatter(mticker.FuncFormatter(fmt_clp_ax))

plt.tight_layout()
plt.savefig("fig1_distribucion_precios.png", dpi=150, bbox_inches="tight")
plt.show()

# ─────────────────────────────────────────────────────────────────────────────
# FIGURA 2 — Evolución mensual por producto (incluye los 6 productos automáticamente)
# ─────────────────────────────────────────────────────────────────────────────
fig2, ax2 = plt.subplots(figsize=(14, 6))

for prod in PRODUCTOS_INTERES:
    sub   = df[df["producto"] == prod].groupby("mes_num")["precio_normalizado"]
    media = sub.mean()
    q1_s  = sub.quantile(0.25)
    q3_s  = sub.quantile(0.75)
    color = PALETA_PRODUCTOS[prod]
    ax2.plot(media.index, media.values, marker="o", color=color,
             linewidth=2, label=prod)
    ax2.fill_between(media.index, q1_s.values, q3_s.values,
                     alpha=0.10, color=color)  # Bajamos un poco el alpha para no saturar con 6 bandas

ax2.set_title("Evolución Mensual de Precios por Producto\n"
              "Línea = media  |  Banda = IQR (50% central de los precios)")
ax2.set_xticks(range(1, 13))
ax2.set_xticklabels(MESES_NOMBRES, rotation=30)
ax2.set_ylabel("Precio (CLP)")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_clp_ax))
ax2.legend(title="Producto", fontsize=10, loc="upper right")
ax2.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig("fig2_evolucion_mensual_productos.png", dpi=150, bbox_inches="tight")
plt.show()

# ─────────────────────────────────────────────────────────────────────────────
# FIGURA 3 — Skewness por dimensión (incluye barras para Tomate y Papa)
# ─────────────────────────────────────────────────────────────────────────────
fig3, axes3 = plt.subplots(1, 2, figsize=(14, 5))
fig3.suptitle("Asimetría (Skewness) por Dimensión\n"
              "|skew| > 1  →  zona roja  →  usar Spearman en correlaciones",
              fontsize=12, fontweight="bold")

skew_prod = (df[df["producto"].isin(PRODUCTOS_INTERES)]
             .groupby("producto")["precio_normalizado"]
             .skew().sort_values())
colores_prod = ["#e74c3c" if abs(v) > 1 else "#2ecc71" for v in skew_prod.values]
bars = axes3[0].barh(skew_prod.index, skew_prod.values,
                     color=colores_prod, edgecolor="white", height=0.55)
axes3[0].axvline(x=1,  color="#e74c3c", linestyle="--", linewidth=1.2, alpha=0.7)
axes3[0].axvline(x=-1, color="#e74c3c", linestyle="--", linewidth=1.2, alpha=0.7)
axes3[0].axvline(x=0,  color="gray",    linestyle="-",  linewidth=0.8, alpha=0.5)
for bar, val in zip(bars, skew_prod.values):
    axes3[0].text(val + 0.02, bar.get_y() + bar.get_height() / 2,
                  f"{val:+.3f}", va="center", fontsize=9)
axes3[0].set_title("Skewness por Producto de Interés")
axes3[0].set_xlabel("Skewness")
axes3[0].set_xlim(skew_prod.min() - 0.3, skew_prod.max() + 0.5)

skew_mes    = df.groupby("mes_num")["precio_normalizado"].skew().sort_index()
colores_mes = ["#e74c3c" if abs(v) > 1 else "#2ecc71" for v in skew_mes.values]
axes3[1].bar(skew_mes.index, skew_mes.values,
             color=colores_mes, edgecolor="white", width=0.7)
axes3[1].axhline(y=1,  color="#e74c3c", linestyle="--", linewidth=1.2, alpha=0.7)
axes3[1].axhline(y=-1, color="#e74c3c", linestyle="--", linewidth=1.2, alpha=0.7)
axes3[1].axhline(y=0,  color="gray",    linestyle="-",  linewidth=0.8, alpha=0.5)
axes3[1].set_xticks(range(1, 13))
axes3[1].set_xticklabels(MESES_NOMBRES, rotation=30)
axes3[1].set_title("Skewness por Mes")
axes3[1].set_xlabel("Mes")
axes3[1].set_ylabel("Skewness")
axes3[1].set_ylim(0, skew_mes.max() * 1.18)
for mes, val in zip(skew_mes.index, skew_mes.values):
    axes3[1].text(mes, val + 0.03, f"{val:.2f}",
                  ha="center", fontsize=8)

parche_ok  = mpatches.Patch(color="#2ecc71", label="|skew| <= 1 — Pearson válido")
parche_mal = mpatches.Patch(color="#e74c3c", label="|skew| > 1  — preferir Spearman")
fig3.legend(handles=[parche_ok, parche_mal],
            loc="lower center", ncol=2, fontsize=10,
            frameon=True, bbox_to_anchor=(0.5, -0.04))
plt.tight_layout()
plt.savefig("fig3_skewness_dimensiones.png", dpi=150, bbox_inches="tight")
plt.show()

# ─────────────────────────────────────────────────────────────────────────────
# FIGURA 4 — Matrices de correlación Spearman por producto (CAMBIO CRÍTICO A 3x2)
# ─────────────────────────────────────────────────────────────────────────────
numeric_cols_corr = [
    c for c in [
        "precio_normalizado", "mes_num", "estacionalidad_index",
        "temperatura", "humedad", "precipitaciones", "radiacion_uv",
        "tasa_cambio", "precio_petroleo_contexto"
    ]
    if c in df.columns and df[c].notna().sum() > 10
]

# Modificamos la grilla a un lienzo de 3 filas y 2 columnas (3x2 = 6 subplots exactos)
fig4, axes4 = plt.subplots(3, 2, figsize=(18, 20))
fig4.suptitle("Matrices de Correlación de Spearman por Producto\n"
              "(Spearman por asimetría detectada en Paso 3)",
              fontsize=13, fontweight="bold")

for ax, prod in zip(axes4.flat, PRODUCTOS_INTERES):
    sub_prod = df[df["producto"] == prod][numeric_cols_corr].dropna()
    if len(sub_prod) < 10:
        ax.set_visible(False)
        continue
    corr = sub_prod.corr(method="spearman")
    mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
    sns.heatmap(
        corr, mask=mask, annot=True, fmt=".2f", cmap="coolwarm",
        square=True, linewidths=0.4, ax=ax,
        vmin=-1, vmax=1,
        cbar_kws={"shrink": 0.75},
        annot_kws={"size": 8}
    )
    precio_idx = list(corr.columns).index("precio_normalizado")
    for j, val in enumerate(corr["precio_normalizado"]):
        if j != precio_idx and abs(val) > 0.3:
            ax.add_patch(plt.Rectangle((precio_idx, j), 1, 1,
                                        fill=False, edgecolor="#f39c12",
                                        lw=2, clip_on=False))
    n_obs = len(sub_prod)
    ax.set_title(f"{prod}  (n={n_obs:,})".replace(",", "."),
                 fontsize=11, fontweight="bold",
                 color=PALETA_PRODUCTOS[prod])
    ax.tick_params(axis="x", rotation=45, labelsize=8)
    ax.tick_params(axis="y", rotation=0,  labelsize=8)

fig4.text(0.5, -0.01,
          "Recuadro naranja = correlación |r| > 0.3 con precio_normalizado",
          ha="center", fontsize=10, style="italic")
plt.tight_layout()
plt.savefig("fig4_correlaciones_spearman.png", dpi=150, bbox_inches="tight")
plt.show()

# ─────────────────────────────────────────────────────────────────────────────
# FIGURA 5 — Entorno macroeconómico vs precio agrícola mediano
# ─────────────────────────────────────────────────────────────────────────────
df_mercado_check = df[
    df["tasa_cambio"].notna() & df["precio_petroleo_contexto"].notna()
]

if not df_mercado_check.empty:
    tasa_serie     = df_mercado_check.groupby("mes_num")["tasa_cambio"].mean()
    petroleo_serie = df_mercado_check.groupby("mes_num")["precio_petroleo_contexto"].mean()
    precio_agr     = df.groupby("mes_num")["precio_normalizado"].median()

    fig5, ax_tasa = plt.subplots(figsize=(14, 5))

    color_agr = "#2ecc71"
    ax_tasa.plot(precio_agr.index, precio_agr.values,
                 marker="o", color=color_agr, linewidth=2.5,
                 label="Precio agrícola mediano (eje izq.)")
    ax_tasa.set_ylabel("Precio Mediano Agrícola (CLP)", color=color_agr, fontsize=10)
    ax_tasa.tick_params(axis="y", labelcolor=color_agr)
    ax_tasa.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_clp_ax))

    ax_tc      = ax_tasa.twinx()
    color_tasa = "#e74c3c"
    ax_tc.plot(tasa_serie.index, tasa_serie.values,
               marker="s", color=color_tasa, linewidth=1.8,
               linestyle="--", label="USD/CLP (eje der.)", alpha=0.85)
    ax_tc.set_ylabel("Tasa de Cambio USD/CLP", color=color_tasa, fontsize=10)
    ax_tc.tick_params(axis="y", labelcolor=color_tasa)
    ax_tc.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_clp_ax))
    ax_tc.set_ylim(tasa_serie.min() * 0.985, tasa_serie.max() * 1.015)

    # Petróleo normalizado al rango del precio agrícola para comparar tendencia
    pet_norm   = ((petroleo_serie - petroleo_serie.min()) /
                  (petroleo_serie.max() - petroleo_serie.min()))
    agr_range  = precio_agr.max() - precio_agr.min()
    pet_escal  = precio_agr.min() + pet_norm * agr_range
    ax_tasa.plot(petroleo_serie.index, pet_escal.values,
                 marker="^", color="#34495e", linewidth=1.8,
                 linestyle=":", alpha=0.75,
                 label="Petróleo (normalizado, eje izq.)")
    
    # Manejo dinámico de anotación en caso de faltar meses indexados
    if len(pet_escal) >= 9:
        ax_tasa.annotate("Petróleo\n(escala norm.)",
                         xy=(9, pet_escal.iloc[8]),
                         xytext=(9.5, pet_escal.iloc[8] + agr_range * 0.09),
                         fontsize=8, color="#34495e",
                         arrowprops=dict(arrowstyle="->", color="#34495e", lw=0.8))

    ax_tasa.set_title("Entorno Macroeconómico vs Precio Agrícola Mediano\n"
                      "(petróleo normalizado para comparación visual de tendencia)",
                      fontsize=11, fontweight="bold")
    ax_tasa.set_xticks(range(1, 13))
    ax_tasa.set_xticklabels(MESES_NOMBRES, rotation=30)
    ax_tasa.grid(True, alpha=0.3)

    lineas1, etiq1 = ax_tasa.get_legend_handles_labels()
    lineas2, etiq2 = ax_tc.get_legend_handles_labels()
    ax_tasa.legend(lineas1 + lineas2, etiq1 + etiq2,
                   loc="upper right", fontsize=9)

    plt.tight_layout()
    plt.savefig("fig5_entorno_macroeconomico.png", dpi=150, bbox_inches="tight")
    plt.show()

print("\n" + "=" * 80)
print("PASO 5: CLUSTERING — SEGMENTACIÓN POR PRODUCTO DE INTERÉS")
print("=" * 80)
print("Objetivo: identificar qué variables climáticas y macroeconómicas")
print("caracterizan los grupos de precio para uva, palta, naranja, limón, papa y tomate.")

# ── Constantes alineadas con la pregunta de negocio (Ampliadas a 6 productos)
PRODUCTOS_CLUSTERING = ["Uva", "Palta", "Naranja", "Limón", "Papa", "Tomate"]
PALETA_PRODUCTOS     = {
    "Uva": "#8e44ad",     # Morado
    "Palta": "#27ae60",   # Verde
    "Naranja": "#e67e22", # Naranja
    "Limón": "#f1c40f",   # Amarillo
    "Papa": "#795548",    # Marrón / Café (Tierra)
    "Tomate": "#c0392b"   # Rojo (Tomate)
}
MES_NOMBRES = {
    1: "enero",    2: "febrero",  3: "marzo",    4: "abril",
    5: "mayo",     6: "junio",    7: "julio",    8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
}

# Variables explicativas alineadas con la pregunta de negocio
# precio_normalizado INCLUIDO: los clusters se forman considerando el precio
VARS_MODELO = [
    "precio_normalizado",       # variable objetivo — forma parte del cluster
    "temperatura",              # variable climática
    "precipitaciones",          # variable climática
    "humedad",                  # variable climática
    "radiacion_uv",             # variable climática
    "tasa_cambio",              # variable macroeconómica
    "precio_petroleo_contexto", # variable macroeconómica/logística
    "mes_num",                  # estacionalidad
]
VARS_CATEGORICAS = ["calidad", "lugar_monitoreo"]

# Verificar que el producto de interés existe en el DataFrame
productos_disponibles = (df["producto"].dropna().unique().tolist()
                         if "producto" in df.columns else [])
productos_a_procesar  = [p for p in PRODUCTOS_CLUSTERING
                         if p in productos_disponibles]

faltantes = set(PRODUCTOS_CLUSTERING) - set(productos_a_procesar)
if faltantes:
    print(f"\n⚠️  Productos no encontrados en el DataFrame: {faltantes}")
print(f"\n📦 Productos a procesar: {productos_a_procesar}")

# ── Diccionario para almacenar resultados (útil para análisis posterior)
resultados_clustering = {}

# ─────────────────────────────────────────────────────────────────────────────
# BUCLE PRINCIPAL — un modelo K-Means por producto de interés
# ─────────────────────────────────────────────────────────────────────────────
for prod in productos_a_procesar:
    print("\n" + "=" * 70)
    print(f"  PRODUCTO: {prod.upper()}")
    print("=" * 70)

    df_prod = df[df["producto"] == prod].copy()

    # ── Variables disponibles para este producto específico
    cols_num = [c for c in VARS_MODELO
                if c in df_prod.columns and df_prod[c].notna().sum() > 5]
    cols_cat = [c for c in VARS_CATEGORICAS if c in df_prod.columns]

    df_base = df_prod[cols_num + cols_cat].dropna().copy()
    n_valid = len(df_base)
    print(f"  Registros válidos para entrenamiento: {n_valid:,}")

    if n_valid < 10:
        print(f"  ⚠️  Datos insuficientes. Mínimo requerido: 10. Omitiendo.")
        continue

    # ── Transformación logarítmica al precio antes de escalar
    # Justificación: el Paso 3 detectó asimetría positiva en todos los productos.
    # Log-transform reduce el efecto de outliers y acerca la distribución a la
    # normalidad, mejorando la cohesión interna de los clusters K-Means.
    df_base = df_base.copy()
    df_base["precio_log"] = np.log1p(df_base["precio_normalizado"])
    cols_num_modelo = [c if c != "precio_normalizado" else "precio_log"
                       for c in cols_num]

    # ── Preprocesamiento
    scaler  = StandardScaler()
    X_num   = scaler.fit_transform(df_base[cols_num_modelo])

    if cols_cat:
        ohe   = OneHotEncoder(sparse_output=False, handle_unknown="ignore")
        X_cat = ohe.fit_transform(df_base[cols_cat])
        X     = np.hstack([X_num, X_cat])
    else:
        X     = X_num

    # ── Búsqueda del K óptimo por Silhouette Score
    max_k = min(7, n_valid - 1)
    if max_k < 3:
        print(f"  ⚠️  Volumen insuficiente para evaluar múltiples K. Omitiendo.")
        continue

    K_range        = range(2, max_k)
    sil_scores     = []
    inertia_scores = []   # guardamos inercia también para referencia (codo)

    for k in K_range:
        km  = KMeans(n_clusters=k, random_state=42, n_init=10)
        lbl = km.fit_predict(X)
        sil_scores.append(silhouette_score(X, lbl))
        inertia_scores.append(km.inertia_)

    k_opt   = K_range[np.argmax(sil_scores)]
    sil_max = max(sil_scores)
    print(f"  K óptimo: {k_opt} clusters  |  Silhouette Score máximo: {sil_max:.3f}")

    # Calidad del clustering: advertir si el score es bajo
    if sil_max < 0.25:
        print(f"  ⚠️  Silhouette Score bajo (<0.25): los clusters no están")
        print(f"      bien separados. Interpretar con cautela.")
    elif sil_max < 0.50:
        print(f"  ℹ️  Silhouette Score moderado (0.25–0.50): separación razonable.")
    else:
        print(f"  ✅ Silhouette Score bueno (>0.50): clusters bien definidos.")

    # ── Gráfica Silhouette + Codo (dos paneles por producto)
    fig_s, axs = plt.subplots(1, 2, figsize=(12, 4))
    color_prod  = PALETA_PRODUCTOS.get(prod, "#2ecc71")

    axs[0].plot(list(K_range), sil_scores, marker="o",
                color=color_prod, linewidth=2)
    axs[0].axvline(x=k_opt, color="#e74c3c", linestyle="--",
                    label=f"K optimo = {k_opt}")
    axs[0].set_title(f"Silhouette Score — {prod}")
    axs[0].set_xlabel("Numero de Clusters (K)")
    axs[0].set_ylabel("Silhouette Score")
    axs[0].legend()
    axs[0].grid(True, alpha=0.4)

    axs[1].plot(list(K_range), inertia_scores, marker="s",
                color=color_prod, linewidth=2, linestyle="--")
    axs[1].axvline(x=k_opt, color="#e74c3c", linestyle="--",
                    label=f"K optimo = {k_opt}")
    axs[1].set_title(f"Metodo del Codo — {prod}")
    axs[1].set_xlabel("Numero de Clusters (K)")
    axs[1].set_ylabel("Inercia (WCSS)")
    axs[1].legend()
    axs[1].grid(True, alpha=0.4)

    plt.suptitle(f"Seleccion de K optimo — {prod}", fontweight="bold", y=1.02)
    plt.tight_layout()
    nombre_sil = f"eda_silhouette_{prod.lower().replace(' ', '_')}.png"
    plt.savefig(nombre_sil, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Grafica exportada: {nombre_sil}")

    # ── Modelo final con K óptimo
    km_final  = KMeans(n_clusters=k_opt, random_state=42, n_init=10)
    df_result = df_prod.loc[df_base.index].copy()
    df_result["cluster"]     = km_final.fit_predict(X)
    df_result["precio_log"]  = df_base["precio_log"].values

    precio_global = df_result["precio_normalizado"].mean()
    mediana_global = df_result["precio_normalizado"].median()

    # ── Perfilamiento por cluster — responde la pregunta de negocio
    print(f"\n  PERFILAMIENTO DE CLUSTERS — {prod.upper()}")
    print(f"  Media global: ${precio_global:.0f}  |  Mediana global: ${mediana_global:.0f}")
    print(f"  Variables en el modelo: {cols_num_modelo}")

    # Calcular importancia relativa de cada variable numérica:
    centroides_orig = pd.DataFrame(
        km_final.cluster_centers_[:, :len(cols_num_modelo)],
        columns=cols_num_modelo
    )
    var_entre_centroides = centroides_orig.var(axis=0)
    importancia_vars     = (var_entre_centroides / var_entre_centroides.sum() * 100).round(1)
    importancia_vars     = importancia_vars.sort_values(ascending=False)

    print(f"\n  CONTRIBUCION DE VARIABLES A LA SEPARACION DE CLUSTERS:")
    print(f"  (varianza entre centroides — mayor % = mayor poder de separacion)")
    for var, pct in importancia_vars.items():
        nombre_display = "precio" if var == "precio_log" else var
        barra = "█" * int(pct / 2)
        print(f"   {nombre_display:<30} {barra:<25} {pct:.1f}%")

    for i in range(k_opt):
        c = df_result[df_result["cluster"] == i]
        precio_c  = c["precio_normalizado"].mean()
        mediana_c = c["precio_normalizado"].median()
        cv_c      = c["precio_normalizado"].std() / precio_c if precio_c > 0 else 0
        n_c       = len(c)

        # Canales y calidades
        lugar_dist = (
            " | ".join([f"{k}: {v*100:.1f}%"
                        for k, v in c["lugar_monitoreo"]
                        .value_counts(normalize=True).items()])
            if "lugar_monitoreo" in c.columns else "N/A"
        )
        calidad_dist = (
            " | ".join([f"{k}: {v*100:.1f}%"
                        for k, v in c["calidad"]
                        .value_counts(normalize=True).items()])
            if "calidad" in c.columns else "N/A"
        )
        mes_tipico = (
            MES_NOMBRES.get(int(c["mes_num"].mode().iat[0]), "N/A")
            if "mes_num" in c.columns and not c["mes_num"].mode().empty else "N/A"
        )

        # Variables climáticas y macroeconómicas del cluster
        temp_c   = c["temperatura"].mean()         if "temperatura"              in c.columns else float("nan")
        hum_c    = c["humedad"].mean()              if "humedad"                  in c.columns else float("nan")
        prec_c   = c["precipitaciones"].mean()      if "precipitaciones"          in c.columns else float("nan")
        uv_c     = c["radiacion_uv"].mean()         if "radiacion_uv"             in c.columns else float("nan")
        tasa_c   = c["tasa_cambio"].mean()          if "tasa_cambio"              in c.columns else float("nan")
        petro_c  = c["precio_petroleo_contexto"].mean() if "precio_petroleo_contexto" in c.columns else float("nan")

        # Comparar variables del cluster con la media global del producto
        def delta(val, col):
            if col not in df_result.columns or np.isnan(val):
                return ""
            ref  = df_result[col].mean()
            pct  = (val - ref) / ref * 100 if ref != 0 else 0
            signo = "↑" if pct > 5 else ("↓" if pct < -5 else "~")
            return f" ({signo}{abs(pct):.1f}% vs global)"

        # Etiqueta basada en precio
        if precio_c > precio_global * 1.12:
            nivel_precio = "PRECIO ALTO"
        elif precio_c < precio_global * 0.88:
            nivel_precio = "PRECIO BAJO"
        else:
            nivel_precio = "PRECIO REGULAR"

        # Identificar la variable no-precio más separadora en este cluster
        vars_explicativas = [v for v in importancia_vars.index if v != "precio_log"]
        var_top = vars_explicativas[0] if vars_explicativas else "N/A"

        print(f"\n  Cluster {i}  ({n_c:,} registros — {n_c/len(df_result)*100:.1f}%)")
        print(f"  ├─ Precio        : media=${precio_c:.0f}\tmediana=${mediana_c:.0f}\tCV={cv_c:.2f}\t→ {nivel_precio}")
        print(f"  ├─ Epoca         : {mes_tipico}")
        print(f"  ├─ Canales      : {lugar_dist}")
        print(f"  ├─ Calidades    : {calidad_dist}")
        print(f"  ├─ Climatologia : Temp={temp_c:.1f}°C{delta(temp_c,'temperatura')} | "
              f"Hum={hum_c:.1f}%{delta(hum_c,'humedad')} | "
              f"Prec={prec_c:.1f}mm{delta(prec_c,'precipitaciones')} | "
              f"UV={uv_c:.2f}{delta(uv_c,'radiacion_uv')}")
        print(f"  ├─ Macro        : USD/CLP={tasa_c:.1f}{delta(tasa_c,'tasa_cambio')} | "
              f"Petroleo=${petro_c:.0f}{delta(petro_c,'precio_petroleo_contexto')}")
        print(f"  └─ Var. top     : '{var_top}' (mayor separacion entre clusters)")

    # Guardar resultado para análisis posterior
    resultados_clustering[prod] = {
        "df_result" : df_result,
        "k_opt"     : k_opt,
        "sil_max"   : sil_max,
        "importancia": importancia_vars,
        "km_model"  : km_final,
    }

print("\n" + "=" * 80)
print("✅ CLUSTERING COMPLETADO.")
print(f"   Productos procesados: {list(resultados_clustering.keys())}")
print(f"   Resultados disponibles en: resultados_clustering[producto]")
print("=" * 80)