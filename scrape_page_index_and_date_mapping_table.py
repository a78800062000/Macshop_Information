from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scrape_page_index_and_date_mapping_table',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

def find_latest_url():
    """找到最新的 MacShop URL."""
    try:
        # 访问当前可见的最新页面
        response = requests.get('https://www.ptt.cc/bbs/MacShop/index.html')
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 找到“上一页”链接
        prev_link = soup.find('a', string='‹ 上頁')
        if prev_link:
            prev_page_url = prev_link['href']
            # 提取页码
            prev_page_number = int(prev_page_url.split('index')[1].split('.html')[0])
            # 最新页码是上一页页码+1
            latest_page_number = prev_page_number + 1
            latest_url = f'https://www.ptt.cc/bbs/MacShop/index{latest_page_number}.html'
            return latest_url
        else:
            raise ValueError("未能找到上一页的链接")
    except Exception as e:
        print(f"发生错误: {e}")
        return None

def scrape_page(page_url):
    """爬取單一頁面並返回日期資料."""
    response = requests.get(page_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    posts = soup.find_all('div', class_='r-ent')
    dates = [post.find('div', class_='date').text.strip() for post in posts if post.find('div', class_='date')]
    return {'URL': page_url, 'Start_Date': min(dates), 'End_Date': max(dates)} if dates else None

def parallel_scrape(**kwargs):
    """平行爬取所有頁面並存儲數據至 JSON，每條數據為單獨一行."""
    latest_url = kwargs['ti'].xcom_pull(task_ids='get_latest_url')
    page_urls = [f'https://www.ptt.cc/bbs/MacShop/index{i}.html' for i in range(1, int(latest_url.split('index')[1].split('.html')[0]) + 1)]
    
    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(scrape_page, page_urls))
    
    results = [result for result in results if result is not None]
    with open('/tmp/macshop_index_and_dates_mapping.json', 'w', encoding='utf-8') as f:
        for result in results:
            json.dump(result, f)
            f.write('\n')  # 確保每個 JSON 對象獨立一行




# 定義 DAG 任務
get_latest_url = PythonOperator(
    task_id='get_latest_url',
    python_callable=find_latest_url,
    dag=dag
)

scrape_and_save = PythonOperator(
    task_id='parallel_scrape_and_save',
    python_callable=parallel_scrape,
    dag=dag
)

bucket_name = 'macshop_data'

upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src='/tmp/macshop_index_and_dates_mapping.json',
    dst='macshop_index_and_dates_mapping.json',
    bucket=bucket_name,
    gcp_conn_id="Connect_Goolge",
    dag=dag
)

# 在 Bigquery用 GCS的 csv file產生 Table
# 如果 Table不存在就自動產生
# 如果 Table存在就刪除後產生新的
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

schema_fields = [
        {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Start_Date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'End_Date', 'type': 'DATE', 'mode': 'NULLABLE'}
]

load_json_to_bigquery = GCSToBigQueryOperator(
    task_id='load_json_to_bigquery',
    bucket=bucket_name,
    source_objects=['macshop_index_and_dates_mapping.json'],
    destination_project_dataset_table='encoded-axis-415404.Macshop_Data.Info_Macshop_Index_and_Dates_Mapping',
    write_disposition='WRITE_TRUNCATE',  # 如果表存在，则删除并重新创建
    skip_leading_rows=0,  # JSON 格式不需跳過首行
    #schema_fields=schema_fields,
    source_format='NEWLINE_DELIMITED_JSON',
    gcp_conn_id="Connect_Goolge",
    dag=dag,
)

# 設定任務依賴
get_latest_url >> scrape_and_save >> upload_to_gcs >> load_json_to_bigquery