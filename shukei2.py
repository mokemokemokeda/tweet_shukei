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
from datetime import datetime
import io
import os
import json

# 環境変数からサービスアカウントキーを取得
google_credentials_json = os.getenv("GOOGLE_SERVICE_ACCOUNT")
if not google_credentials_json:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT が設定されていません。")
json_data = json.loads(google_credentials_json)

# Google Drive API クライアントの作成
credentials = service_account.Credentials.from_service_account_info(json_data)
drive_service = build("drive", "v3", credentials=credentials)

# ファイルIDを取得
def get_file_id(file_name):
    query = f"name = '{file_name}' and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None

# Google SheetsをExcelに変換して取得
def download_google_sheets_file(file_id):
    request = drive_service.files().export_media(fileId=file_id, mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh

# Chromeオプション
CHROME_OPTIONS = Options()
CHROME_OPTIONS.add_argument('--headless=new')
CHROME_OPTIONS.add_argument('--no-sandbox')
CHROME_OPTIONS.add_argument('--disable-dev-shm-usage')

# 「もっと見る」ボタンの操作
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

# ツイート要素の収集
def extract_tweet_elements(driver, max_tweets=100):
    while True:
        tweet_elements = driver.find_elements(By.XPATH, '//div[contains(@class, "Tweet_TweetContainer")]')
        if len(tweet_elements) >= max_tweets or not find_show_more_button(driver):
            break
        click_show_more_button(driver)
    return tweet_elements[:max_tweets]

# 本文・スクリーンネーム・時刻を抽出
def extract_tweet_data(tweet_elements):
    tweet_data = []
    for tweet_element in tweet_elements:
        try:
            text = tweet_element.find_element(By.XPATH, './/div[contains(@class, "Tweet_body")]').text
        except NoSuchElementException:
            text = ''
        try:
            screen_name = tweet_element.find_element(By.XPATH, './/a[contains(@class, "Tweet_authorID")]').text.lstrip('@')
        except NoSuchElementException:
            screen_name = ''
        try:
            time_text = tweet_element.find_element(By.XPATH, './/time').text
        except NoSuchElementException:
            time_text = ''
        tweet_data.append({
            'Tweet': text,
            'ScreenName': screen_name,
            'TweetTime': time_text
        })
    return tweet_data

# メイン処理
keyword = '#プリオケ'
url_encoded_keyword = urllib.parse.quote(keyword)
all_tweet_data = []

driver = webdriver.Chrome(options=CHROME_OPTIONS)

try:
    for i in range(2):  # 繰り返し回数
        print(f"{i+1}回目の取得中...")
        driver.get(f'https://search.yahoo.co.jp/realtime/search?p={url_encoded_keyword}')
        time.sleep(2)

        # 自動更新タブの処理（初回のみ出ることがある）
        try:
            tab_element = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//div[contains(@class, "Tab_")]'))
            )
            tab_element.click()
            time.sleep(1)
        except TimeoutException:
            pass

        # ツイート取得
        tweet_elements = extract_tweet_elements(driver, max_tweets=100)
        tweet_data = extract_tweet_data(tweet_elements)
        all_tweet_data.extend(tweet_data)

        print(f"　取得ツイート数: {len(tweet_data)}")
        time.sleep(5)
finally:
    driver.quit()

print("全取得完了。総ツイート数:", len(all_tweet_data))

# Drive上の履歴ファイル更新
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

# 新データ追加
new_data = pd.DataFrame(all_tweet_data)
history_df = pd.concat([history_df, new_data], ignore_index=True)

# Excelとしてアップロード
with io.BytesIO() as fh:
    with pd.ExcelWriter(fh, engine='xlsxwriter') as writer:
        history_df.to_excel(writer, index=False)
    fh.seek(0)
    media = MediaIoBaseUpload(fh, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    if history_id:
        drive_service.files().update(fileId=history_id, media_body=media).execute()
    else:
        file_metadata = {"name": history_file, "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
        drive_service.files().create(body=file_metadata, media_body=media).execute()
