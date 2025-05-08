import pandas as pd
import requests
import time
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from datetime import datetime, timedelta
import io
import os
import json
import re

# 環境変数からサービスアカウントキーを取得
google_credentials_json = os.getenv("GOOGLE_SERVICE_ACCOUNT")
if not google_credentials_json:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT が設定されていません。")
json_data = json.loads(google_credentials_json)

# Googleサービスアカウントデータ
credentials = service_account.Credentials.from_service_account_info(json_data)
drive_service = build("drive", "v3", credentials=credentials)

# Google Drive からファイル ID を取得する関数
def get_file_id(file_name):
    query = f"name = '{file_name}' and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None

# Google SheetsをExcelに変換してダウンロード
def download_google_sheets_file(file_id):
    request = drive_service.files().export_media(fileId=file_id, mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh

# Chromeオプション設定
CHROME_OPTIONS = Options()
CHROME_OPTIONS.add_argument('--headless=new')
CHROME_OPTIONS.add_argument('--no-sandbox')
CHROME_OPTIONS.add_argument('--disable-dev-shm-usage')

# ツイート時間の正規化
def parse_tweet_time(tweet_time_str):
    now = datetime.utcnow() + timedelta(hours=9)  # 日本時間
    if "分前" in tweet_time_str:
        minutes_ago = int(re.search(r'(\d+)分前', tweet_time_str).group(1))
        return (now - timedelta(minutes=minutes_ago)).strftime('%Y-%m-%d %H:%M')
    elif "時間前" in tweet_time_str:
        hours_ago = int(re.search(r'(\d+)時間前', tweet_time_str).group(1))
        return (now - timedelta(hours=hours_ago)).strftime('%Y-%m-%d %H:%M')
    elif "昨日" in tweet_time_str:
        match = re.search(r'昨日\s*(\d{1,2}):(\d{2})', tweet_time_str)
        if match:
            hour, minute = int(match.group(1)), int(match.group(2))
            tweet_time = datetime.utcnow().replace(hour=hour, minute=minute, second=0, microsecond=0) - timedelta(days=1)
            adjusted = tweet_time + timedelta(hours=9)
            return (tweet_time if adjusted > now else adjusted).strftime('%Y-%m-%d %H:%M')
    else:
        match = re.search(r'(\d{1,2})/(\d{1,2})\s*(\d{1,2}):(\d{2})', tweet_time_str)
        if match:
            month, day, hour, minute = map(int, match.groups())
            year = now.year
            tweet_time = datetime(year, month, day, hour, minute)
            adjusted = tweet_time + timedelta(hours=9)
            return (tweet_time if adjusted > now else adjusted).strftime('%Y-%m-%d %H:%M')
    return tweet_time_str

# ツイート取得ロジック
def extract_tweet_elements(driver, max_tweets=100):
    while True:
        tweet_elements = driver.find_elements(By.XPATH, '//div[contains(@class, "Tweet_TweetContainer")]')
        if len(tweet_elements) >= max_tweets or not find_show_more_button(driver):
            break
        click_show_more_button(driver)
    return tweet_elements[:max_tweets]

def extract_tweets(driver, max_tweets=100):
    tweets_data = []
    tweet_elements = extract_tweet_elements(driver, max_tweets=max_tweets)
    for tweet_element in tweet_elements:
        try:
            body = tweet_element.find_element(By.XPATH, './/div[contains(@class, "Tweet_body")]').text
            screen_name_elem = tweet_element.find_element(By.XPATH, './/span[contains(@class, "TweetScreenName")]')
            screen_name = screen_name_elem.text.lstrip('@')  # @除去
            time_elem = tweet_element.find_element(By.XPATH, './/time')
            raw_time = time_elem.text
            parsed_time = parse_tweet_time(raw_time)
            tweets_data.append({
                "Tweet": body,
                "ScreenName": screen_name,
                "TweetTime": parsed_time
            })
        except NoSuchElementException:
            continue
    return tweets_data

def find_show_more_button(driver):
    try:
        return driver.find_element(By.XPATH, '//button[contains(@class, "More_")]')
    except NoSuchElementException:
        return None

def click_show_more_button(driver):
    button = find_show_more_button(driver)
    if button:
        button.click()
        time.sleep(2)
        return True
    return False

# 実行本体
keyword = '#プリオケ'
url_encoded_keyword = urllib.parse.quote(keyword)
all_tweets = []

driver = webdriver.Chrome(options=CHROME_OPTIONS)

try:
    for i in range(2):
        print(f"{i+1}回目の取得中...")
        driver.get(f'https://search.yahoo.co.jp/realtime/search?p={url_encoded_keyword}')
        time.sleep(2)

        try:
            tab = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//div[contains(@class, "Tab_")]'))
            )
            tab.click()
            time.sleep(1)
        except TimeoutException:
            pass

        tweets = extract_tweets(driver, max_tweets=100)
        all_tweets.extend(tweets)
        print(f"　取得ツイート数: {len(tweets)}")
        time.sleep(5)

finally:
    driver.quit()

print("全取得完了。総ツイート数:", len(all_tweets))

# Google Driveへの保存処理
history_file = "Priorche_tweet_shukei.xlsx"
history_id = get_file_id(history_file)

if history_id:
    file_metadata = drive_service.files().get(fileId=history_id).execute()
    mime_type = file_metadata['mimeType']
    if mime_type == "application/vnd.google-apps.spreadsheet":
        history_df = pd.read_excel(download_google_sheets_file(history_id))
    else:
        history_df = pd.read_excel(f"https://drive.google.com/uc?id={history_id}")
else:
    history_df = pd.DataFrame(columns=["Tweet", "ScreenName", "TweetTime"])

new_data = pd.DataFrame(all_tweets)
history_df = pd.concat([history_df, new_data], ignore_index=True)

with io.BytesIO() as fh:
    with pd.ExcelWriter(fh, engine='xlsxwriter') as writer:
        history_df.to_excel(writer, index=False)
    fh.seek(0)
    media = MediaIoBaseUpload(fh, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    if history_id:
        drive_service.files().update(fileId=history_id, media_body=media).execute()
    else:
        file_metadata = {"name": history_file, "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheet"}
        drive_service.files().create(body=file_metadata, media_body=media).execute()
