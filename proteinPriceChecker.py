from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import sqlite3
import datetime as dt
import requests
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import logging

#bitlyのトークン
bitly_token = ""
#lineトークン
lineToken = ""

#ログの設定
logging.basicConfig(filename='', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

def main():
    try:
        # Chromeドライバーを自動更新
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)

        #DB接続
        dname = "product.db"
        conn = sqlite3.connect(dname)
        cur = conn.cursor()

        #テーブルチェック
        dbTableCheck(cur)

        #商品データ取得
        products = getProducts(conn)

        for product in products:
            # スクレイピングする商品のURLを指定
            url = product['product_url']
            # 商品ページを開く
            driver.get(url)
            # 商品タイトルを取得
            #title = driver.find_element(By.ID, 'productTitle').text.strip()

            # 商品価格を取得
            price_xpath = '//*[@id="corePrice_feature_div"]/div/span/span[2]'
            price_element = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, price_xpath))
            )
            currentPrice = price_element.text.strip()
            convertCurrentPrice = currentPrice[1:6].replace(",", "")

            now = dt.datetime.now()

            #SQL実行
            conn.execute("INSERT INTO Prices (product_id, price, scraping_datetime) VALUES (?, ?, ?)", (product['product_id'], convertCurrentPrice, now))

            # データベースに保存
            conn.commit()
                
            #同じ商品が90レコード以上あれば、最古のレコードを削除する
            delete_old_records(cur, product['product_id'])

            cur.execute("SELECT * FROM Prices where product_id=?", (product['product_id'],))
            rows = cur.fetchall()

            threeMonthsAgo = dt.datetime.now() - dt.timedelta(days=90)

            #同じ商品の過去三ヶ月間の価格を取得
            cur.execute("SELECT * FROM Prices WHERE scraping_datetime >= ? AND product_id = ?", (threeMonthsAgo, product['product_id']))
            rows = cur.fetchall()

            #最安置を取得
            minPrice = 99999999
            for row in rows:
                pastPrice = row[2]
                if pastPrice < minPrice:
                    minPrice = pastPrice

            # 現在の価格が最安値以下であれば、LINEに通知する
            if int(convertCurrentPrice) <= minPrice:
                #URL短縮メソッド
                url = bitly_shorten_url(url)
                #product_idをもとに商品名を取得
                product_name = product['product_name']
                
                message = '{}が最安値を更新しました！\n現在の価格：{}円\n過去3ヶ月の最安値：{}円\nURL:{}'.format(product_name, convertCurrentPrice, minPrice, url)
                payload = {'message': message}
                headers = {'Authorization': 'Bearer ' + lineToken}
                requests.post('https://notify-api.line.me/api/notify', data=payload, headers=headers)
                print("LINEに最安値の報告をしました！")

        # データベースをクローズ
        conn.close()

        # ブラウザを閉じる
        driver.quit()

        logging.info('Scraping completed successfully.')

    except Exception as e:
        logging.error(f'Error occurred: {e}')

def dbTableCheck(cur):
    # テーブルが存在しない場合は作成する
    # Productsテーブルを作成する
    cur.execute('''CREATE TABLE IF NOT EXISTS Products
             (product_id INTEGER PRIMARY KEY AUTOINCREMENT,
             product_name TEXT NOT NULL,
             product_url TEXT NOT NULL);''')

    # Pricesテーブルを作成する
    cur.execute('''CREATE TABLE IF NOT EXISTS Prices
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             product_id INTEGER NOT NULL,
             price INTEGER NOT NULL,
             scraping_datetime DATETIME,
             FOREIGN KEY (product_id) REFERENCES Products(product_id));''')
    
def getProducts(conn):
    #Productsテーブルから全レコードを取得する
    cursor = conn.execute("SELECT * from Products")

    # 取得したレコードを配列に格納する
    products = []
    for row in cursor:
        product = {}
        product['product_id'] = row[0]
        product['product_name'] = row[1]
        product['product_url'] = row[2]
        products.append(product)
    return products

def delete_old_records(cur, product_id):
    cur.execute("SELECT * FROM Prices where product_id=?", (product_id,))
    rows = cur.fetchall()
    if len(rows) >= 90:
        cur.execute("DELETE FROM Prices WHERE id = (SELECT id FROM Prices WHERE product_id = ? ORDER BY id ASC LIMIT 1)", (product_id,))
        print("レコードを削除しました")

#URL短縮
def bitly_shorten_url(url):
    headers = {'Authorization': f'Bearer {bitly_token}'}
    params = {'long_url': url}
    response = requests.post('https://api-ssl.bitly.com/v4/shorten', headers=headers, json=params)
    if response.status_code == 200:
        data = response.json()
        short_url = data['link']
        logging.info(f'Short URL: {short_url}')
        return short_url
    else:
        logging.error(f'Error: {response.status_code} - {response.text}')
        return url
        
if __name__ == "__main__":
    main()