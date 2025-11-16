from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import boto3
from botocore.client import Config

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def fetch_cities_from_web():
    """Скачиваем города СНГ"""
    url = "https://raw.githubusercontent.com/datasets/world-cities/master/data/world-cities.csv"
    
    try:
        df = pd.read_csv(url)
        CIS_COUNTRIES = ['Russia', 'Ukraine', 'Belarus', 'Kazakhstan', 'Uzbekistan', 
                        'Azerbaijan', 'Armenia', 'Georgia', 'Moldova', 'Tajikistan', 
                        'Turkmenistan', 'Kyrgyzstan']
        
        cis_cities = df[df['country'].isin(CIS_COUNTRIES)]
        
        cities_list = []
        for _, row in cis_cities.iterrows():
            cities_list.append({
                'name': row['name'],
                'country': row['country'],
                'lat': 0,
                'lon': 0,
                'population': 100000
            })
        
        print(f"Найдено {len(cis_cities)} городов СНГ")
        return cities_list[:100]
        
    except Exception as e:
        print(f"Ошибка: {e}")
        return []

def save_to_s3(**context):
    """Сохраняем данные в S3 Bronze используя boto3 напрямую"""
    try:
        # Получаем данные из предыдущей задачи
        cities_data = context['task_instance'].xcom_pull(task_ids='fetch_cis_cities')
        print(f"Получено {len(cities_data)} городов для сохранения")
        
        # Создаем клиент boto3 для MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        print("S3 клиент создан успешно")
        
        bucket_name = "fire-data"
        
        # Проверяем существует ли бакет
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Бакет {bucket_name} существует")
        except:
            print(f"Создаю бакет {bucket_name}...")
            s3_client.create_bucket(Bucket=bucket_name)
        
        # Сохраняем как JSON
        file_name = f"cities_cis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        json_data = json.dumps(cities_data, ensure_ascii=False, indent=2)
        
        # Загружаем в S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"bronze/cities/{file_name}",
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"✅ Данные сохранены в S3: {file_name}")
        print("Проверь файл в MinIO: http://localhost:9001")
        return file_name
        
    except Exception as e:
        print(f"❌ Ошибка при сохранении в S3: {e}")
        import traceback
        print(f"Детали ошибки: {traceback.format_exc()}")
        return None

with DAG(
    dag_id='cis_cities_to_s3',
    default_args=default_args,
    description='Сбор городов СНГ и сохранение в S3',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_cis_cities',
        python_callable=fetch_cities_from_web
    )
    
    save_task = PythonOperator(
        task_id='save_to_s3_bronze',
        python_callable=save_to_s3,
        provide_context=True
    )
    
    fetch_task >> save_task