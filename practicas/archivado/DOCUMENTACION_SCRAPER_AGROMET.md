# Documentación Scraper Agromet - Gabriel Tenorio

## Información General

**Proyecto**: Scraper de Datos Climáticos - Agromet Chile  
**Equipo**: G9_Agrotech  
**Integrante**: Gabriel Tenorio  
**Universidad**: UCN  
**Curso**: Big Data para la toma de decisiones  

---

## Historial de Cambios

### Versión 3.0 (29-04-2026)
- ✅ Ampliación temporal: 2024-2025 (antes solo 2025)
- ✅ Ampliación geográfica: 2 comunas (antes solo 1)
- ✅ Reestructuración del esquema de datos (1 registro por día vs. 2 registros por día)
- ✅ Nuevas etiquetas: año, fecha, comuna, estacion
- ✅ Formato de fechas chileno (DD-MM-YYYY)

### Versión 2.0 (Producción anterior)
- Extracción año 2025 completo
- Solo comuna de Coquimbo
- Esquema con campo "etiqueta" (temperatura/precipitación separadas)

---

## Configuración de Estaciones

### Región de Coquimbo (ID: 5)

#### Estación 1: Guayacán (Coquimbo)
- **Comuna ID**: 4102
- **Comuna Nombre**: Coquimbo
- **Estación ID**: 345
- **Estación Nombre**: Guayacán
- **Variables extraídas**:
  - Temperatura promedio del aire (°C)
  - Precipitación horaria (mm)

#### Estación 2: Cerro Grande (La Serena)
- **Comuna ID**: 4101
- **Comuna Nombre**: La Serena
- **Estación ID**: 323
- **Estación Nombre**: Cerro Grande
- **Variables extraídas**:
  - Temperatura promedio del aire (°C)
  - Precipitación horaria (mm)

---

## Endpoints API Agromet

### Endpoint Principal - Datos Históricos
```
POST https://www.agromet.cl/ext/aux/getDatosHistoricosEstacion_histo_estacion.php
```

**Headers requeridos**:
```
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
X-Requested-With: XMLHttpRequest
Referer: https://www.agromet.cl/datos-historicos
User-Agent: Mozilla/5.0 (compatible; ScraperAgrotech/1.0)
```

**Parámetros**:
- `combo_reg_ia_id`: ID de región (5)
- `combo_com_ia_id`: ID de comuna (4102 o 4101)
- `combo_ema_ia_id`: ID de estación (345 o 323)
- `id_variable`: Tipo de variable ("TEMP_MED")
- `fechaInicio`: Fecha inicio formato "DD/MM/YYYY HH:MM"
- `fechaFin`: Fecha fin formato "DD/MM/YYYY HH:MM"

### Endpoints de Consulta (Auxiliares)
```
GET https://www.agromet.cl/ext/aux/getComunasRegion.php?reg_ia_id=5
GET https://www.agromet.cl/ext/aux/getFilteredEMAS.php?reg_ia_id=5
GET https://www.agromet.cl/ext/aux/getFilteredEMAS.php?reg_ia_id=5&com_ia_id=4102
```

---

## Limitaciones Técnicas de la API

### Restricción temporal
- **Máximo por petición**: 3 meses consecutivos
- **Solución implementada**: División en trimestres (Q1, Q2, Q3, Q4)

### Rate Limiting
- **Delay entre peticiones**: 1 segundo
- **Razón**: Evitar bloqueo por exceso de peticiones (error 429)
- **Total de peticiones**: 8 trimestres × 2 estaciones = 16 peticiones

---

## Esquema de Datos de Salida

### Estructura JSON/CSV (v3.0)
```python
{
    "integrante": str,        # "gabriel tenorio" (fijo)
    "mes": str,               # "Enero", "Febrero", etc.
    "año": int,               # 2024 o 2025
    "fecha": str,             # "15-01-2025" (formato DD-MM-YYYY)
    "fecha_captura": str,     # "29-04-2026" (fecha de ejecución)
    "comuna": str,            # "Coquimbo" o "La Serena"
    "estacion": str,          # "Guayacán" o "Cerro Grande"
    "temperatura": float,     # Promedio diario en °C (2 decimales)
    "precipitaciones": float  # Suma diaria en mm (2 decimales)
}
```

### Granularidad
- **Nivel temporal**: 1 registro por día
- **Agregación temperatura**: Promedio de mediciones horarias del día
- **Agregación precipitación**: Suma de precipitaciones horarias del día

---

## Cobertura de Datos

### Temporal
- **Años**: 2024 y 2025
- **Trimestres**:
  - Q1: 01/01 - 31/03
  - Q2: 01/04 - 30/06
  - Q3: 01/07 - 30/09
  - Q4: 01/10 - 31/12

### Geográfica
- **Región**: Coquimbo (ID: 5)
- **Comunas**: 2 (Coquimbo, La Serena)
- **Estaciones**: 2 (Guayacán, Cerro Grande)

### Volumen estimado de datos
- **Días por año**: ~365
- **Años**: 2
- **Estaciones**: 2
- **Total registros esperados**: ~1,460 registros

---

## Pipeline de Procesamiento

### 1. Extracción (API Requests)
```
Para cada estación:
  Para cada trimestre (2024 + 2025):
    - Realizar petición POST a Agromet
    - Extraer tabla HTML con BeautifulSoup
    - Convertir a DataFrame de pandas
    - Esperar 1 segundo (rate limiting)
```

### 2. Transformación
```
- Limpieza de valores numéricos (reemplazo de comas, manejo de "--")
- Conversión a tipos numéricos
- Derivación de columnas: fecha, mes
- Agrupación por día (groupby fecha)
- Cálculo de agregaciones (mean, sum)
```

### 3. Estandarización
```
- Aplicar esquema del equipo G9_Agrotech
- Formato de fechas chileno (DD-MM-YYYY)
- Redondeo a 2 decimales
- Agregar metadatos (integrante, comuna, estación, fecha_captura)
```

### 4. Salida
```
- Generación de CSV con timestamp
- Codificación UTF-8 con BOM (utf-8-sig)
- Archivo: agromet_datos_YYYYMMDD_HHMMSS.csv
```

---

## Manejo de Errores

### Errores contemplados
- ✅ Errores de conexión (RequestException)
- ✅ Tablas HTML no encontradas
- ✅ Valores numéricos malformados
- ✅ Rate limiting (429 Too Many Requests)
- ✅ Timeouts

### Estrategia
- Continuar con siguiente trimestre si uno falla
- Logging descriptivo con emojis (✅, ⚠️, ❌)
- Verificación de datos antes de concatenación

---

## Dependencias Python

```python
requests        # Peticiones HTTP
urllib3         # Manejo SSL
pandas          # Procesamiento de datos
beautifulsoup4  # Parsing HTML
datetime        # Timestamps
warnings        # Supresión de warnings SSL
time            # Rate limiting
pymongo         # Inserción a db local para verificación
```

---

## Notas de Implementación

### Configuración flexible
Los nombres de comunas y estaciones están parametrizados en la configuración `ESTACIONES`. Para cambiar el formato de estos nombres, modificar directamente en esa estructura.

### Formato de fechas
El formato DD-MM-YYYY se aplica mediante `strftime('%d-%m-%Y')`. Para cambiar a otro formato (ej: YYYY-MM-DD), modificar el string de formato en la sección de generación de `fecha` y `fecha_captura`.

### Extensibilidad
Para agregar más comunas/estaciones:
1. Consultar endpoints auxiliares para obtener IDs
2. Agregar entrada en `ESTACIONES`
3. El scraper iterará automáticamente sobre todas las configuraciones

---

## Validación de Salida

### Checks recomendados
- ✅ Total de registros ≈ días × estaciones
- ✅ Todos los meses representados (Enero-Diciembre)
- ✅ Años correctos (2024, 2025)
- ✅ Sin valores NULL en campos obligatorios
- ✅ Formato de fechas consistente (DD-MM-YYYY)
- ✅ Redondeo correcto (2 decimales)

---

## Contacto y Soporte

**Integrante responsable**: Gabriel Tenorio  
**Equipo**: G9_Agrotech  
**Universidad**: UCN  
**Fuente de datos**: [Agromet - Red Agroclimática Nacional](https://www.agromet.cl)

---

*Última actualización: 29-04-2026*
