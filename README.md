# Hito 1: Scraping y Almacenamiento en MongoDB

## Configuración del entorno Docker

### Arquitectura de servicios (docker-compose.yml)

El proyecto utiliza una arquitectura multi-contenedor orquestada con `docker-compose.yml` que incluye los siguientes servicios:

#### 1. **workspace** - Entorno de desarrollo principal

Contenedor basado en el Dockerfile personalizado que incluye Jupyter Lab, PySpark, Selenium y entorno gráfico (VNC/noVNC).

**Configuración:**
- **Container name**: `bigdata_workspace`
- **Platform**: `linux/amd64`
- **Puertos expuestos**:
  - `8888`: Jupyter Lab
  - `4040`: Interfaz web de Spark UI
  - `5900`: Acceso VNC (escritorio remoto)
  - `6080`: Acceso noVNC (navegador)
- **Volúmenes**: Monta el directorio del proyecto en `/home/jovyan/work`
- **Memoria compartida**: `2gb` (evita fallos de Chrome/Selenium)
- **DNS**: `8.8.8.8` (Google DNS para resolución de MongoDB Atlas)

**Variables de entorno clave:**
```yaml
JUPYTER_TOKEN=                    # Sin autenticación
JUPYTER_PASSWORD=                 # Sin contraseña
JUPYTER_ENABLE_LAB=yes            # Fuerza JupyterLab
JUPYTER_ALLOW_INSECURE_WRITES=true
DISPLAY=:99                       # Display virtual para GUI
```

#### 2. **database** - MongoDB Local

Base de datos MongoDB local para desarrollo y pruebas.

**Configuración:**
- **Imagen**: `mongo:latest`
- **Container name**: `bigdata_mongodb`
- **Puerto**: `27017:27017`
- **Volumen persistente**: `mongo_data:/data/db`


### Red personalizada

Todos los servicios están conectados a una red bridge personalizada:

```yaml
networks:
  bigdata_net:
    driver: bridge
    name: BigData_Network_UCN
```

### Resumen de puertos

| Puerto | Servicio | Descripción |
|--------|----------|-------------|
| `8888` | Jupyter Lab | Entorno de desarrollo interactivo |
| `4040` | Spark UI | Monitoreo de trabajos Spark |
| `5900` | VNC | Acceso remoto al escritorio (cliente VNC) |
| `6080` | noVNC | Acceso remoto al escritorio (navegador) |
| `27017` | MongoDB Local | Base de datos local |
| `8081` | Mongo Express Local | Admin MongoDB Local |
| `8082` | Mongo Express Atlas | Admin MongoDB Atlas |
| `8083` | FileBrowser | Explorador de archivos web |

---

## Comando de ejecución

Para levantar todos los contenedores en modo detached (segundo plano):

```bash
docker-compose up -d
```

Para detener todos los servicios:

```bash
docker-compose down
```

Para reconstruir las imágenes (después de modificar el Dockerfile):

```bash
docker-compose up -d --build
```

---

## Evidencias del Hito 1

### Evidencia 1: Consumo de recursos (docker stats)

![Docker Stats](./documentacion/evidencias/docker_stats.png)

*Captura mostrando el uso de CPU, memoria y red de los contenedores en ejecución.*

---

### Evidencia 2: Conteo de documentos en MongoDB

![MongoDB Count](./documentacion/evidencias/mongodbAtlas_count.jpeg)

---

## Tabla de Atributos Extraídos por Integrante

| Integrante | Atributos/Etiquetas extraídas |
|------------|-------------------------------|
| Maximiliano Berrios | `integrante`, `precio_producto`, `fecha`,`mes`, `año`, `variacion`, `fecha_captura` |
| Sebastían Castillo  | `integrante`, `producto`, `comuna`,`variedad`,`calidad`, `lugar_monitoreo`, `fecha`, `mes`, `año`, `precio`, `fecha_captura`, `fertilizante`, `tasa_cambio` |
| Lissete Mathieu | `integrante`, `producto`, `comuna`,`variedad`,`calidad`, `lugar_monitoreo`, `fecha`, `mes`, `año`, `precio`, `fecha_captura`, `fertilizante`, `tasa_cambio` |
| Alejandro Nuñez | `integrante`, `producto`, `comuna`,`variedad`,`calidad`, `lugar_monitoreo`, `fecha`, `mes`, `año`, `precio`, `fecha_captura`, `fertilizante`, `tasa_cambio` |
| Gabriel Tenorio | `integrante`, `mes`, `año`,`fecha`, `fecha_captura`, `comuna`,`estacion`, `temperatura`, `precipitacion`  |
| Matheieus Villavicencio | `integrante`, `mes`, `año`,`fecha`, `fecha_captura`, `radiacion_uv`,`humedad`, `comuna` |

*Nota: Debido a que se utilizaron las mismas páginas (Odepa e Indeximundi) para ciertas variables pero para distintos productos, se repiten las etiquetas para los siguientes integrantes:*
- Alejandro Nuñez -> Limones, naranjas.
- Sebastían Castillo -> Tomates, papas.
- Lissete Matheiu -> Uvas, paltas  


---

## Acceso a los servicios

Una vez levantados los contenedores, los servicios están disponibles en:

- **Jupyter Lab**: http://localhost:8888
- **Spark UI**: http://localhost:4040 (solo cuando hay trabajos ejecutándose)
- **noVNC (Escritorio)**: http://localhost:6080
- **MongoDB Local (Admin)**: http://localhost:8081
- **MongoDB Atlas (Admin)**: http://localhost:8082
- **File Browser**: http://localhost:8083

---

## Notas adicionales

### Conexión a MongoDB desde notebooks

**MongoDB Local:**
```python
from pymongo import MongoClient

client = MongoClient("mongodb://database:27017/")
db = client["nombre_base_datos"]
collection = db["nombre_coleccion"]
```

**MongoDB Atlas:**
```python
from pymongo import MongoClient

uri = "mongodb+srv://agrotech_sebastiancastillo:agrotechbigdata2026@cluster0.7z77rka.mongodb.net/BigData_UCN?retryWrites=true&w=majority"
client = MongoClient(uri)
db = client["nombre_base_datos"]
collection = db["nombre_coleccion"]
```
# Librerías utilizadas → Dockerfile

El contenedor Docker del proyecto está basado en `jupyter/pyspark-notebook:latest` e incluye las siguientes dependencias y componentes:

## Paquetes del sistema (apt-get)

- **Navegador y automatización web:**
  - `google-chrome-stable` - Navegador para scraping con Selenium
  - `libnss3`, `libgbm1`, `libasound2` - Dependencias de Chrome

- **Entorno gráfico y VNC:**
  - `xvfb` - X Virtual Framebuffer (display virtual)
  - `fluxbox` - Gestor de ventanas ligero
  - `x11vnc` - Servidor VNC para acceso remoto
  - `novnc` - Cliente VNC basado en web
  - `python3-websockify` - Proxy WebSocket para noVNC
  - `supervisor` - Gestor de procesos

- **Utilidades generales:**
  - `wget`, `curl` - Descarga de archivos
  - `gnupg`, `ca-certificates`, `openssl` - Certificados y seguridad
  - `unzip` - Descompresión de archivos
  - `sed` - Editor de texto stream

## Drivers web (Selenium)

- **ChromeDriver** - Instalado manualmente desde `chrome-for-testing-public`, versión sincronizada automáticamente con la versión de Chrome instalada (se elimina dependencia de `webdriver-manager` para mayor estabilidad)

## JARs de Spark (MongoDB Connector)

Versión **10.3.0** compatible con Spark 3.5:

- `mongo-spark-connector_2.12-10.3.0.jar`
- `mongodb-driver-sync-4.11.1.jar`
- `mongodb-driver-core-4.11.1.jar`
- `bson-4.11.1.jar`
- `bson-record-codec-4.11.1.jar`

## Librerías de Python (pip)

- **`pymongo[srv]`** - Driver de MongoDB con soporte para conexiones Atlas (SRV)
- **`dnspython`** - Resolución DNS requerida por pymongo[srv]
- **`selenium`** - Framework de automatización web para scraping
- **`pandas`** - Manipulación y análisis de datos
- **`certifi`** - Certificados SSL/TLS de Mozilla

## Diferencias con el Dockerfile del profesor

| Componente | Dockerfile profesor | Dockerfile proyecto |
|------------|---------------------|---------------------|
| **ChromeDriver** | Instalado vía `webdriver-manager` (Python) | Instalado **manualmente** desde repositorio oficial de Google, sincronizado con versión de Chrome |
| **`unzip`** | ❌ No incluido | ✅ Incluido (necesario para descomprimir ChromeDriver) |
| **Permisos Jupyter** | ⚠️ Posibles problemas de escritura | ✅ Configuración explícita de directorios `/home/jovyan/.local`, `.jupyter`, `.ipython` |
| **`webdriver-manager`** | ✅ Instalado | ❌ **Eliminado** para evitar conflictos con ChromeDriver manual |

### Justificación de cambios

1. **ChromeDriver manual**: Evita problemas de compatibilidad entre versiones de Chrome y ChromeDriver que pueden surgir con `webdriver-manager`.
2. **Fix de permisos**: Asegura que Jupyter pueda escribir en sus directorios de configuración sin errores de permisos.
3. **Eliminación de `webdriver-manager`**: Reduce conflictos al tener un solo método de gestión del driver.
