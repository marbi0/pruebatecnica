import os
import requests
import json
from datetime import datetime, timezone
from google.cloud import bigquery

class APIDownloader:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def get_data(self) -> list:
        print(f"Descargando datos de {self.endpoint}...")
        response = requests.get(self.endpoint)
        response.raise_for_status() 
        data = response.json()

        return data[:100]
    
    def save_to_local_json(self, data: list, file_path: str):
        folder = os.path.dirname(file_path)
        
        if folder and not os.path.exists(folder):
            os.makedirs(folder)
            print(f"Carpeta creada en: {folder}")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        
        print(f"Archivo guardado exitosamente en: {file_path}")

class BQUploader:
    def __init__(self, project_id: str, dataset_id: str, table_name: str):
        # las credenciales se cogen autimaticamente de Google Cloud CLI
        self.client = bigquery.Client(project=project_id)
        self.table_id = f"{project_id}.{dataset_id}.{table_name}"

    def upload_json(self, data: list):
        print(f"Subiendo {len(data)} registros a {self.table_id}...")
        
        timestamp_extraccion = datetime.now(timezone.utc).isoformat()
        for row in data:
            row['fecha_extraccion'] = timestamp_extraccion

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        job = self.client.load_table_from_json(data, self.table_id, job_config=job_config)
        job.result()
        
        print(f"Carga completada. Total de filas en destino: {job.output_rows}")


if __name__ == "__main__":

    PROJECT_ID = "project-ee2f21b1-cde8-4ee4-b57" 
    DATASET_SANDBOX = "SANDBOX_PRUEBA_ETL"
    TABLE_SANDBOX = "api_posts_raw"
    API_URL = "https://jsonplaceholder.typicode.com/posts"
    PATH_LOCAL_JSON = "Parte_2/data/api_data.json"

    try:
        # Paso 1: Descargar datos del la API
        downloader = APIDownloader(API_URL)
        api_data = downloader.get_data()
        downloader.save_to_local_json(api_data, PATH_LOCAL_JSON)

        # Paso 2: Subir datos a GCP
        uploader = BQUploader(PROJECT_ID, DATASET_SANDBOX, TABLE_SANDBOX)
        uploader.upload_json(api_data)
        
    except Exception as e:
        print(f"Ocurrió un error durante la ejecución: {e}")