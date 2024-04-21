from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import csv
from google.cloud import storage
import os

# 定義預設參數
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定義 DAG
dag = DAG(
    'ptt_scraping_to_gcs',
    default_args=default_args,
    description='從PTT抓取數據並存儲到Google Cloud Storage',
    schedule_interval=timedelta(days=1),
)

def get_soup(url, **kwargs):
    try:
        response = requests.get(url, cookies={'over18': '1'}, timeout=10)
        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser')
        else:
            print(f"Error fetching page: Status code {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    
def get_previous_page_url(soup, **kwargs):
    controls = soup.find('div', class_='action-bar').find('div', class_='btn-group btn-group-paging')
    prev_link = controls.find_all('a')[1]['href'] if controls else None
    if prev_link:
        return 'https://www.ptt.cc' + prev_link
    return None

def get_article_content(article_url, **kwargs):
    soup = get_soup(article_url)
    if soup is None:
        return "無法檢索文章內容", "日期資訊未找到"

    date_info = soup.find_all('div', class_='article-metaline')[-1].find('span', class_='article-meta-value').text if soup.find_all('div', class_='article-metaline') else "日期資訊未找到"

    content = soup.find(id='main-content')
    for meta in content.select('div.article-metaline, div.article-metaline-right, div.push'):
        meta.decompose()

    content_text = content.text.strip()

    try:
        start_index = content_text.index('[售價]')
        end_index = content_text.index('[交易方式/地點]', start_index)
        price_info = content_text[start_index:end_index].strip()
        return price_info, date_info
    except ValueError:
        return "文章中未找到價格資訊或交易方式資訊。", date_info

def scrape_ptt_and_upload_to_gcs(**kwargs):
    # 目標日期，自行設定
    target_date = datetime.strptime('2024-03-01', '%Y-%m-%d')
    # 初始化為 MacShop 版最新頁面的 URL
    current_url = 'https://www.ptt.cc/bbs/MacShop/index.html'
    results = []

    while current_url:
        response = requests.get(current_url, cookies={'over18': '1'}, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        should_stop = False
        posts = soup.find_all('div', class_='r-ent')
        for post in posts:
            title_link = post.find('div', class_='title').a
            if title_link is None:
                continue
            title = title_link.text.strip()
            date_str = post.find('div', class_='date').text.strip()
            post_date = datetime.strptime(date_str + ' 2024', '%m/%d %Y')

            if post_date < target_date:
                should_stop = True
                break

            article_url = 'https://www.ptt.cc' + title_link['href']
            # price_info, creation_date = get_article_content(article_url)
            # results.append([article_url, title, price_info, creation_date])
            results.append([article_url, title, post_date])

            # 打印爬取到的数据
            print(f"URL: {article_url}")
            print(f"標題: {title}")
            # print(f"價格: {price_info}")
            print(f"創建日期: {post_date}")
            print("----------------")

        if should_stop:
            break

        # 尋找上一頁的URL
        current_url = get_previous_page_url(soup)

    # 保存爬取結果到CSV
    csv_file = '/tmp/ptt_scraped_url_and_title.csv'
    with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        # writer.writerow(['URL', 'Title', 'Price', 'Creation Date'])
        writer.writerow(['URL', 'Title', 'Post_Date'])
        writer.writerows(results)

scrape_task = PythonOperator(
    task_id='scrape_ptt_and_upload_to_gcs',
    python_callable=scrape_ptt_and_upload_to_gcs,
    dag=dag,
)

# 資料傳到 GCS，成為 CSV
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
bucket_name = "macshop_data" # 修改為你的桶名

upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src='/tmp/ptt_scraped_url_and_title.csv',  # 本地文件路径
    dst='ptt_scraped_url_and_title.csv',  # GCS 上的文件路径
    bucket=bucket_name,
    gcp_conn_id="Connect_Goolge",  # 使用在 Airflow 中设置的连接 ID
    dag=dag,
)

# 在 Bigquery用 GCS的 csv file產生 Table
# 如果 Table不存在就自動產生
# 如果 Table存在就刪除後產生新的
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

load_csv_to_bigquery_task = GCSToBigQueryOperator(
    task_id='load_csv_to_bigquery',
    bucket=bucket_name,
    source_objects=['ptt_scraped_url_and_title.csv'],
    destination_project_dataset_table='encoded-axis-415404.Macshop_Data.Log_Macshop_url',
    schema_fields=None,  # 设置为None以自动检测schema
    write_disposition='WRITE_TRUNCATE',  # 如果表存在，则删除并重新创建
    skip_leading_rows=1,  # 跳过标题行
    time_partitioning={'type': 'DAY', 'field': 'Post_Date'},
    cluster_fields=['Post_Date'],
    gcp_conn_id="Connect_Goolge",
    dag=dag,
)

# 设置任务依赖关系
scrape_task >> upload_to_gcs_task >> load_csv_to_bigquery_task