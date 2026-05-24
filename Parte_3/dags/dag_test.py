from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import BaseOperator

# 4) CREAMOS EL OPERADOR PERSONALIZADO: TimeDiff
class TimeDiffOperator(BaseOperator):
    
    def __init__(self, diff_date, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if isinstance(diff_date, str):
            self.diff_date = datetime.fromisoformat(diff_date)
        else:
            self.diff_date = diff_date

    def execute(self, context):
        now = datetime.now()
        diff = now - self.diff_date
        
        self.log.info(f"Fecha de referencia ingresada: {self.diff_date}")
        self.log.info(f"Fecha de ejecución actual: {now}")
        self.log.info(f"--> Diferencia de tiempo: {diff}")
        
        return str(diff)

# 1) DEFINIMOS EL DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(1900, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id='test',
    default_args=default_args,
    schedule_interval='0 3 * * *', 
    catchup=False,
    description='DAG de Prueba Técnica'
) as dag:

    # 2) INCLUIMOS LAS TAREAS start Y end COMO DummyOperator
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # 3) LISTA DE N TAREAS DUMMY CON DEPENDENCIAS
    N = 5 
    
    odd_tasks = []
    even_tasks = []

    for i in range(1, N + 1):
        task = DummyOperator(task_id=f'task_{i}')
        if i % 2 != 0:
            odd_tasks.append(task)
        else:
            even_tasks.append(task)

    start >> odd_tasks

    for even_task in even_tasks:
        odd_tasks >> even_task 

    # 4) DEFINIMOS LA NUEVA TAREA
    time_diff_task = TimeDiffOperator(
        task_id='calculate_time_difference',
        diff_date=datetime(2023, 1, 1) 
    )

    if even_tasks:
        even_tasks >> time_diff_task
    else:
        odd_tasks >> time_diff_task

    time_diff_task >> end

# 5) PREGUNTAS
"""
¿Que es un Hook? ¿En que se diferencia de una conexion?

- Conexion: Es un conjunto de parametros de configuracion que definen donde y con que credenciales nos vamos a conectar.
  
- Hook: Es una clase de Python que tiene la logica para conectarse e interactuar con un sistema externo como una base de datos o APIs. 
  
En resumen: Se diferencian en que la conexion almacena la informacion de acceso y autenticacion 
y el hook llama a la conexion y realiza operaciones en el sistema al que nos hayamos conectado.
"""