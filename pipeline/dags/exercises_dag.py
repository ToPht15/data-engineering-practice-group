from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'exercises_pipeline',
    default_args=default_args,
    description='Pipeline to run Exercise-1 through Exercise-5 sequentially',
    schedule_interval=None,  
    start_date=datetime(2025, 4, 26),
    catchup=False,
    tags=['exercises'],
) as dag:
    exercise_tasks = []
    
    for i in range(1, 6):
        exercise_name = f"Exercise-{i}"
        
        with TaskGroup(group_id=exercise_name) as exercise_group:
            run_exercise = BashOperator(
                task_id=f'run_{exercise_name.lower()}',
                bash_command=f'cd /opt/airflow/app/{exercise_name} && python main.py' ,
            )
            
            
        exercise_tasks.append(exercise_group)
    
    exercise_tasks[0] >> exercise_tasks[1] >> exercise_tasks[2] >> exercise_tasks[3] >> exercise_tasks[4]