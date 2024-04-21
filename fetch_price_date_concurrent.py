from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pyarrow as pa  # Needed for Parquet file formatting
import pyarrow.parquet as pq  # Needed to save DataFrame as Parquet
import json

# 替换为您的 GCP 项目详细信息
bucket_name = 'macshop_data'
destination_dataset_table = 'encoded-axis-415404.Macshop_Data.Log_Macshop_Data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_soup(url):
    response = requests.get(url)
    if response.status_code == 200:
        return BeautifulSoup(response.text, 'html.parser')
    return None

def get_price_info(article_url, **kwargs):
    soup = get_soup(article_url)
    if soup is None:
        return "無法檢索文章內容"
    
    content = soup.find(id='main-content')
    if content:
        for meta in content.select('div.article-metaline, div.article-metaline-right, div.push'):
            meta.decompose()
        content_text = content.text.strip()
        
        try:
            # 嘗試尋找 '[售價]' 或 '[希望價格]' 開始的位置
            start_price_index = content_text.find('[售價]')
            start_want_price_index = content_text.find('[希望價格]')
            
            # 確定最先出現的標籤，並設定為開始位置
            if start_price_index == -1 or (start_want_price_index != -1 and start_want_price_index < start_price_index):
                start_index = start_want_price_index
                start_label_length = len('[希望價格]')
            else:
                start_index = start_price_index
                start_label_length = len('[售價]')
            
            # 尋找 '[交易方式/地點]' 結束的位置
            end_index = content_text.index('[交易方式/地點]', start_index)
            
            # 提取並返回信息，不包含起始和結束的標籤
            price_info = content_text[start_index + start_label_length:end_index].strip()
            return price_info
        except ValueError:
            return "文章中未找到價格資訊或交易方式資訊。"
    else:
        return "無法檢索到文章內容"

def get_date_info(article_url, **kwargs):
    soup = get_soup(article_url)
    if soup is None:
        return None
    
    metalines = soup.find_all('div', class_='article-metaline')
    if not metalines:
        return None  # 如果没有找到任何 metaline 元素，直接返回 None

    last_metaline = metalines[-1]
    date_tag = last_metaline.find('span', class_='article-meta-value')
    if date_tag:
        date_str = date_tag.text.strip()
        try:
            # 假设日期格式是“Tue Mar 27 23:02:15 2024”，则解析为 datetime 对象
            parsed_date = datetime.strptime(date_str, '%a %b %d %H:%M:%S %Y')
            return parsed_date.strftime('%Y-%m-%d %H:%M:%S')  # 格式化日期时间为 'YYYY-MM-DD HH:MM:SS'
        except ValueError:
            return None  # 解析失败返回 None
    return None


from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
hook = BigQueryHook(gcp_conn_id="Connect_Goolge", use_legacy_sql=False)
client = hook.get_client()
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

@dag(default_args=default_args, schedule_interval=timedelta(days=1), start_date=datetime(2024, 4, 9), catchup=False)
def fetch_price_date_concurrent():

    @task
    def fetch_urls_from_gbq():
        query = "SELECT URL FROM `encoded-axis-415404.Macshop_Data.Log_Macshop_url` WHERE TIMESTAMP_TRUNC(Post_Date, DAY) >= TIMESTAMP('2024-03-01')"
        query_job = client.query(query)
        results = query_job.result()
        return [row.URL for row in results]

    @task
    def fetch_and_combine_info(urls):
        def fetch_info(url):
            price_info = get_price_info(url)
            date_info = get_date_info(url)
            return {'url': url, 'price_info': price_info, 'date_info': date_info}

        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(fetch_info, urls))
        return results
    
    @task
    def save_to_json(combined_info):
        df = pd.DataFrame(combined_info)
        json_file = '/tmp/ptt_scraped_data.json'
        df.to_json(json_file, orient='records', lines=True)
        return json_file

    check_gbq_data = BigQueryCheckOperator(
        task_id='check_gbq_for_data',
        sql="SELECT COUNT(*) FROM `encoded-axis-415404.Macshop_Data.Log_Macshop_url`  WHERE TIMESTAMP_TRUNC(Post_Date, DAY) >= TIMESTAMP('2024-03-01')",
        use_legacy_sql=False,
        gcp_conn_id="Connect_Goolge"
    )

    urls = fetch_urls_from_gbq()
    combined_info = fetch_and_combine_info(urls)
    json_file = save_to_json(combined_info)

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=json_file,
        dst='ptt_scraped_data.json',
        bucket=bucket_name,
        gcp_conn_id="Connect_Goolge"
    )

    load_json_to_gbq = GCSToBigQueryOperator(
        task_id='load_json_to_gbq',
        bucket=bucket_name,
        source_objects=['ptt_scraped_data.json'],
        destination_project_dataset_table=destination_dataset_table,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id="Connect_Goolge",
        time_partitioning={'type': 'DAY', 'field': 'date_info'},  # Partition by date_info
        cluster_fields=['date_info'],  # Cluster by date_info
    )
    
    check_gbq_data >> urls >> combined_info >> json_file >> upload_to_gcs_task >> load_json_to_gbq

dag = fetch_price_date_concurrent()
