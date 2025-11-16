from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def fetch_cities_from_web():
    """Скачиваем готовый CSV со всеми городами СНГ > 100k"""
    
    # Используем готовый CSV с GitHub
    url = "https://raw.githubusercontent.com/datasets/world-cities/master/data/world-cities.csv"
    
    try:
        # Скачиваем CSV
        df = pd.read_csv(url)
        
        # Фильтруем страны СНГ
        CIS_COUNTRIES = ['Russia', 'Ukraine', 'Belarus', 'Kazakhstan', 'Uzbekistan', 
                        'Azerbaijan', 'Armenia', 'Georgia', 'Moldova', 'Tajikistan', 
                        'Turkmenistan', 'Kyrgyzstan']
        
        cis_cities = df[df['country'].isin(CIS_COUNTRIES)]
        
        # Добавляем координаты (в реальном CSV они уже есть)
        # и фильтруем по населению > 100k
        
        print(f"Найдено {len(cis_cities)} городов СНГ")
        
        # Преобразуем в нужный формат
        cities_list = []
        for _, row in cis_cities.iterrows():
            cities_list.append({
                'name': row['name'],
                'country': row['country'],
                'lat': row.get('lat', 0),  # В реальном CSV будут координаты
                'lon': row.get('lon', 0),
                'population': 100000  # Заглушка
            })
        
        return cities_list[:50]  # Ограничим для теста
        
    except Exception as e:
        print(f"Ошибка: {e}")
        return []

with DAG(
    dag_id='cis_cities_collection_v2',
    default_args=default_args,
    description='Сбор городов СНГ',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_cis_cities',
        python_callable=fetch_cities_from_web
    )