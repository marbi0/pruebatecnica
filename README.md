
# Prueba Técnica: Data Engineer (Python, GCP, BigQuery & Airflow)

Este repositorio contiene la solución a la prueba técnica.
El proyecto implementa un flujo completo de extracción (API), carga (Sandbox) y transformación (Integración) de datos en Google Cloud Platform (BigQuery),
finalizando con la orquestación de tareas mediante Apache Airflow.

## 📁 Estructura del Repositorio

```markdown
/
├── Parte_2
│   └── main.py              # Script principal (Extracción y Carga)
│   └── data/
│   │   └── api_data.json       # Datos crudos descargados de la API localmente
│   │   └── Captura.png         # Captura de pantalla con la prueba de la insercion de los datos
│   └── sql/
│   │   └── transform.sql       # Consulta BigQuery
├── Parte_3
│   └── .env                 # Fichero de entorno para Docker
│   └── docker-compose.yaml  # Fichero yaml
│   └── dags/
│   │   └── dag_test.py         # Orquestación y DAG de Apache Airflow
├── README.md                # Documentación del proyecto

```

---

## 🛠️ Requisitos Previos

Para ejecutar este proyecto en tu entorno local, necesitarás:

1. **Python 3.13** instalado.
2. **Google Cloud CLI** (`gcloud`) configurado para la autenticación.
3. **Docker Desktop** (con integración WSL 2 activa si usas Windows) para levantar Airflow.
4. Un proyecto activo en Google Cloud con la API de BigQuery habilitada y dos Datasets creados (`SANDBOX_PRUEBA` e `INTEGRATION`).

---

## 🚀 Instrucciones de Ejecución

### Parte 1: Autenticación con Google Cloud

El script utiliza el estándar moderno de *Application Default Credentials (ADC)* para interactuar con BigQuery sin necesidad de exponer archivos JSON de claves de servicio.

Abre tu terminal y ejecuta:

```bash
# 1. Iniciar sesión en GCP
gcloud auth application-default login

# 2. Configurar tu proyecto de trabajo
gcloud config set project <TU_PROJECT_ID>

```

*(Nota: Reemplaza `<TU_PROJECT_ID>` por tu ID de proyecto real en los scripts).*

### Parte 2: Ejecución del Pipeline (Python + BQ)

El script automatiza la extracción desde la API y la ejecución de la consulta SQL.

```bash
# 1. Instalar las dependencias necesarias
pip install <DEPENDECIA>

# 2. Ejecutar el pipeline principal
python main.py

```

Al finalizar, los datos estarán disponibles en las tablas de `SANDBOX_PRUEBA`, después tendremos que ir a BigQuery y crear la tabla `integration_prueba_tecnica` y ejecutar el sql dentro de BigQuery para tener procesados los datos en la tabla de `INTEGRATION`.

### Parte 3: Orquestación con Apache Airflow

Para evaluar la lógica de dependencias y el operador personalizado, levantaremos un entorno local de Airflow mediante Docker.

```bash
# 1. Descargar el archivo oficial de configuración de Airflow si no se tiene
curl -LfO '[https://airflow.apache.org/docs/apache-airflow/2.8.1/docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/2.8.1/docker-compose.yaml)'

# 2. Levantar los contenedores de Airflow en segundo plano
docker compose up -d

```

1. Accede a `http://localhost:8080` (Credenciales por defecto: `airflow` / `airflow`).
2. Busca el DAG llamado `test`.
3. Activa el DAG (Toggle de Unpause) y pulsa el botón **Trigger** para forzar una ejecución manual y visualizar el grafo de tareas.

---

## 🧠 Explicación de los Componentes

### 1. `main.py` (Extracción y Carga)

Contiene dos clases principales y automatiza todo el proceso:

* **`APIDownloader`:** Se conecta a `https://jsonplaceholder.typicode.com/posts`, extrae los 100 primeros registros y los guarda localmente en la carpeta `data/` con formato UTF-8 para preservar caracteres especiales.
* **`BQUploader`:** Sube el archivo JSON a la tabla *Sandbox*. Añade una marca de tiempo (`fecha_extraccion`) estandarizada en UTC. Además, incorpora métodos para ejecutar DDL (crear la tabla final si no existe) y DML (ejecutar la transformación).

### 2. `transform.sql` (Transformación Idempotente)

Este script utiliza la sentencia `MERGE` combinada con la función analítica `QUALIFY ROW_NUMBER()`.

* **Deduplicación:** Elimina posibles registros duplicados del día garantizando que solo se procese la `fecha_extraccion` más reciente por ID.
* **Idempotencia:** Asegura que ejecutar el script múltiples veces sobre los mismos datos no genere duplicados en la tabla final `integration_prueba_tecnica`, actualizando las filas existentes (Update) o insertando las nuevas (Insert). Añade la columna `fecha_transformacion`.

### 3. `dag_test.py` (Orquestación y Custom Operator)

Define un flujo en Airflow que se ejecuta diariamente a las 03:00 UTC.

* **Dependencias Complejas:** Despliega dinámicamente un número $N$ de tareas *dummy*. Demuestra el uso avanzado del operador bitshift (`>>`) mediante listas para lograr que cada tarea par dependa simultáneamente de todas las tareas impares.
* **`TimeDiffOperator`:** Es un *Custom Operator* que hereda de `BaseOperator`. Recibe una fecha parámetro, calcula la diferencia exacta frente a la hora de ejecución del sistema y lo registra en los *logs* de Airflow.
* **Teoría (Hooks vs Connections):** Incluye en su cabecera la respuesta documentada a la diferencia funcional entre ambos conceptos en la arquitectura de Airflow.
