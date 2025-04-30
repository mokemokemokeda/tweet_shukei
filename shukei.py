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

    # Google Sheets ファイルを Excel にエクスポートしてダウンロードする関数
def download_google_sheets_file(file_id):
    request = drive_service.files().export_media(fileId=file_id, mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh

# Chromeのオプションを設定
CHROME_OPTIONS = Options()
CHROME_OPTIONS.add_argument('--headless=new')
CHROME_OPTIONS.add_argument('--no-sandbox')
CHROME_OPTIONS.add_argument('--disable-dev-shm-usage')

# Chrome WebDriverのインスタンスを作成
driver = webdriver.Chrome(options=CHROME_OPTIONS)

# 検索キーワード
keyword = '#プリオケ'

# URLエンコード
url_encoded_keyword = urllib.parse.quote(keyword)

# Yahooリアルタイム検索ページを開く
driver.get(f'https://search.yahoo.co.jp/realtime/search?p={url_encoded_keyword}')
time.sleep(2)  # ページ読み込み待機

# 「タイムラインの自動更新を停止」ボタンをクリック
try:
    tab_element = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//div[contains(@class, "Tab_")]'))
    )
    tab_element.click()
    time.sleep(1)
except TimeoutException:
    print("タブ要素が見つかりませんでした。スキップします。")

def extract_tweet_texts(tweet_elements):
    """ ツイートのテキストを取得 """
    tweet_texts = []
    for tweet_element in tweet_elements:
        try:
            tweet_text_element = tweet_element.find_element(By.XPATH, './/div[contains(@class, "Tweet_body")]')
            tweet_texts.append(tweet_text_element.text)
        except NoSuchElementException:
            continue
    return tweet_texts

def find_show_more_button(driver):
    """ もっと見るボタンを取得 """
    try:
        return driver.find_element(By.XPATH, '//button[contains(@class, "More_")]')
    except NoSuchElementException:
        return None

def click_show_more_button(driver):
    """ もっと見るボタンをクリック """
    button = find_show_more_button(driver)
    if button:
        button.click()
        time.sleep(2)
        return True
    return False

def extract_tweet_elements(driver, max_tweets=100):
    """ ツイート要素を取得（スクロール＆「もっと見る」クリック） """
    while True:
        tweet_elements = driver.find_elements(By.XPATH, '//div[contains(@class, "Tweet_TweetContainer")]')

        if len(tweet_elements) >= max_tweets or not find_show_more_button(driver):
            break

        click_show_more_button(driver)

    return tweet_elements[:max_tweets]

# ツイートを取得
tweet_elements = extract_tweet_elements(driver, max_tweets=100)

# ツイートのテキストを取得
tweet_texts = extract_tweet_texts(tweet_elements)

print("取得したツイート数:", len(tweet_texts))
#print("\n".join(tweet_texts))  # 改行表示

# WebDriverを閉じる
driver.quit()

# ツイートテキストをDataFrameに変換
df = pd.DataFrame(tweet_texts, columns=['Tweet'])

#print("取得したツイート数:", len(tweet_texts))
#print("\n".join(tweet_texts))  # 改行表示

# WebDriverを閉じる
driver.quit()

# 記録ファイルの取得と更新
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
    history_df = pd.DataFrame()

# A列（ツイート内容）のみ記録
if history_df.empty:
    history_df = pd.DataFrame(columns=["Tweet"])

# new_data DataFrameを作成（ツイートのみ）
new_data = pd.DataFrame({'Tweet': tweet_texts})

# データを追記
history_df = pd.concat([history_df, new_data], ignore_index=True)

# Excelファイルを更新
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
