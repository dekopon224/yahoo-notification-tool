import requests
import pandas as pd
import time
import os
import boto3
import csv
from io import StringIO
import json

def search_yahoo_items(application_id, query, sort='-score', hits=10, start=1, price_from=None, price_to=None, seller_id=None, retries=3):
    """
    Yahoo!ショッピング商品検索APIを使用して、指定されたパラメータに基づいて商品情報を取得する関数。
    """
    base_url = "https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch"

    params = {
        'appid': application_id,
        'query': query,
        'sort': sort,
        'results': hits,
        'start': start
    }

    if price_from is not None:
        params['price_from'] = price_from

    if price_to is not None:
        params['price_to'] = price_to

    if seller_id:
        params['seller_id'] = seller_id

    for attempt in range(retries):
        try:
            print(f"APIリクエスト: query='{query}', price_from={price_from}, price_to={price_to}")
            time.sleep(1)  # Yahoo APIのレート制限に応じた待機時間（1クエリー/秒）

            response = requests.get(base_url, params=params)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 400:
                print(f"Error: {response.status_code}")
                print(f"Response: {response.text}")
                break  # 400エラーはクライアント側の問題なのでリトライしない
            elif response.status_code == 429:
                print(f"Error: {response.status_code} - レート制限に達しました")
                time.sleep(5)  # レート制限の場合は長めに待機
            else:
                print(f"Error: {response.status_code}")
                print(f"Response: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Request Exception: {e}")

        print(f"Retrying... ({attempt + 1}/{retries})")
        time.sleep(2)  # 再試行前の待機時間

    return None

def send_chatwork_notification(room_id, api_token, message, retries=3):
    """
    チャットワークに通知を送信する関数。
    """
    url = f"https://api.chatwork.com/v2/rooms/{room_id}/messages"
    headers = {
        'X-ChatWorkToken': api_token,
    }
    data = {
        'body': message
    }

    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, data=data)
            
            if response.status_code == 200:
                print("チャットワークに通知が送信されました。")
                time.sleep(1)  # レート制限を考慮して1秒待機
                return True
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 5))  # Retry-Afterヘッダーがあれば取得
                print(f"チャットワーク通知エラー: {response.status_code} - レート制限に達しました。{retry_after}秒待機して再試行します。")
                time.sleep(retry_after)
            else:
                print(f"チャットワーク通知エラー: {response.status_code}")
                print(f"Response: {response.text}")
                break  # 他のエラーはリトライしない
        except requests.exceptions.RequestException as e:
            print(f"Chatwork Request Exception: {e}")

        print(f"Retrying... ({attempt + 1}/{retries})")
        time.sleep(2)  # 再試行前の待機時間

    print("チャットワークへの通知に失敗しました。")
    return False

def is_single_character(word):
    """
    単語が1文字かどうかを判定する関数。
    """
    return len(word) == 1

def load_notified_list(s3_client, bucket, key):
    """
    S3から通知済みリストを読み込む関数。
    存在しない場合は空のセットを返す。
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(content))
        notified_set = set(df['itemUrl'].tolist())
        print(f"通知済みリストをロードしました。総アイテム数: {len(notified_set)}")
        return notified_set
    except s3_client.exceptions.NoSuchKey:
        print("notifiedlist.csvが存在しません。新規に作成します。")
        return set()
    except Exception as e:
        print(f"notifiedlist.csvの読み込み中にエラーが発生しました: {e}")
        return set()

def save_notified_list(s3_client, bucket, key, notified_set):
    """
    通知済みリストをS3に保存する関数。
    """
    df = pd.DataFrame({'itemUrl': list(notified_set)})
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"通知済みリストをS3に保存しました。総アイテム数: {len(notified_set)}")
    except Exception as e:
        print(f"notifiedlist.csvの保存中にエラーが発生しました: {e}")

def search_and_notify(config_data, application_id, chatwork_room_id, chatwork_api_token, notified_set):
    """
    Yahoo!ショッピングAPIを使用して商品を検索し、条件に応じてチャットワークに通知する関数。
    通知後、通知済みリストに追加します。
    """
    # CSVの各行に対して処理を行う
    for index, row in config_data.iterrows():
        # 行番号をログとして表示
        print(f"現在 {index + 1} 行目を処理しています。")
        
        original_keyword = str(row['product_keyword']).strip()
        ng_keyword = str(row['ng_keyword']).strip() if pd.notna(row['ng_keyword']) else None
        min_price = row['lowest_price'] if pd.notna(row['lowest_price']) else None
        max_price = row['highest_price'] if pd.notna(row['highest_price']) else None

        # sellerIdExsの処理
        sellerIdExs_raw = str(row['sellerIdExs']).strip() if pd.notna(row['sellerIdExs']) else 'nan'
        if sellerIdExs_raw.lower() != 'nan':
            sellerIdExs = sellerIdExs_raw.strip('"')  # ダブルクォーテーションを除去
            excluded_shops = [shop.strip().lower() for shop in sellerIdExs.split(',')]
            print(f"除外店舗リスト: {excluded_shops}")
        else:
            excluded_shops = []

        # キーワードが空でないか確認
        if not original_keyword:
            print(f"行 {index + 1} のキーワードが空です。スキップします。")
            continue

        # キーワードを単語に分割
        words = original_keyword.split(' ')
        # 1文字の単語を除外
        filtered_words = [word for word in words if not is_single_character(word)]
        # フィルタリング後のキーワードを作成
        if filtered_words:
            modified_keyword = ' '.join(filtered_words)
        else:
            # すべての単語が1文字の場合、元のキーワードを使用（APIエラーを避けるためスキップ）
            print(f"行 {index + 1} のキーワード '{original_keyword}' はすべて1文字の単語です。スキップします。")
            continue

        print(f"元のキーワード: '{original_keyword}'")
        print(f"フィルタリング後のキーワード: '{modified_keyword}'")

        # product_conditionを取得し、通知条件を決定
        product_condition_str = str(row['product_condition']).strip()
        product_conditions = [cond.strip() for cond in product_condition_str.split('|')]

        if '新品、未使用' in product_conditions:
            if len(product_conditions) == 1:
                # Case 1: [新品、未使用]のみ
                notify_condition = 'exclude_chuko'
            else:
                # Case 2: [新品、未使用|他の条件]
                notify_condition = 'include_all'
        else:
            # Case 3: [新品、未使用]を含まない
            notify_condition = 'include_chuko_only'

        print(f"product_condition: {product_conditions}")
        print(f"notify_condition: {notify_condition}")

        # Yahoo!ショッピングAPIで商品を検索
        result = search_yahoo_items(
            application_id=application_id,
            query=modified_keyword,
            price_from=min_price,
            price_to=max_price
        )
        
        if result:
            items = result.get('hits', [])
            if not items:
                print(f"行 {index + 1} のキーワード '{modified_keyword}' に一致する商品は見つかりませんでした。")
                continue

            for item in items:
                item_name = item['name']
                item_url = item['url']
                shop_name = item.get('seller', {}).get('name', '不明').strip().lower()  # 店舗名を取得し、小文字化

                # 除外店舗のチェック
                if shop_name in excluded_shops:
                    print(f"商品 '{item_name}' は除外店舗 '{shop_name}' のため通知しません。")
                    continue

                # 通知済みかどうかをチェック
                if item_url in notified_set:
                    print(f"商品 '{item_name}' は既に通知済みです。スキップします。")
                    continue

                # 全ての単語が商品名に含まれているかチェック（大文字・小文字を無視）
                item_name_lower = item_name.lower()
                all_words_present = all(word.lower() in item_name_lower for word in filtered_words)

                # ng_keywordが設定されている場合、商品名に含まれているかチェック
                if ng_keyword:
                    ng_words = ng_keyword.split()  # 半角スペースで分割
                    # 含まれているng_keywordがあればスキップ
                    if any(ng_word.lower() in item_name_lower for ng_word in ng_words):
                        print(f"商品 '{item_name}' にng_keywordが含まれているため通知しません。")
                        continue

                if all_words_present:
                    # 「中古」のチェックを追加
                    if notify_condition == 'exclude_chuko':
                        if '中古' in item_name:
                            print(f"商品 '{item_name}' は '中古' を含むため通知しません。")
                            continue
                    elif notify_condition == 'include_chuko_only':
                        if '中古' not in item_name:
                            print(f"商品 '{item_name}' は '中古' を含まないため通知しません。")
                            continue
                    # notify_condition が 'include_all' の場合、チェックなしで通知

                    # itemPriceをカンマ区切りにフォーマット
                    try:
                        formatted_price = f"{int(item['price']):,}"  # 例: 1000 -> 1,000
                    except ValueError:
                        # itemPriceが整数でない場合はそのまま使用
                        formatted_price = item['price']

                    # config.csvのnameカラムを参照してダブルクォーテーションを除去
                    name_value = str(row['name']).strip('"')
                    
                    # 通知メッセージの作成（店舗名を先頭に移動）
                    message = (f"店舗名: {shop_name}\n商品名: {item_name}\n価格: {formatted_price}円\n"
                               f"URL: {item_url}\nconfig.csvのnameカラムを参照した表示: {name_value}")

                    # 通知を送信
                    success = send_chatwork_notification(chatwork_room_id, chatwork_api_token, message)
                    
                    if success:
                        # 通知済みリストに追加
                        notified_set.add(item_url)
                        print(f"商品 '{item_name}' を通知済みリストに追加しました。")
                    else:
                        print(f"行 {index + 1} の商品 '{item_name}' の通知送信に失敗しました。")
                    
                    # レート制限を遵守するために1秒待機
                    time.sleep(1)
                else:
                    print(f"商品 '{item_name}' はキーワードの全ての単語を含んでいません。通知は送信されません。")
        
        # 次の行に進む前に少し待機（必要に応じて調整）
        # time.sleep(1)
    
    return notified_set

def lambda_handler(event, context):
    """
    Lambdaのハンドラ関数。
    バッチ処理を行い、必要に応じて次のバッチを再度呼び出す。
    """
    # 環境変数から取得
    application_id = os.getenv('YAHOO_APPLICATION_ID', 'your_yahoo_app_id_2')
    chatwork_room_id = os.getenv('CHATWORK_ROOM_ID', '372041369')
    chatwork_api_token = os.getenv('CHATWORK_API_TOKEN', '2f92f7d4409ac2726c716fc0513fadc1')
    config_s3_bucket = os.getenv('CONFIG_S3_BUCKET', 'config-csv')
    config_s3_key = os.getenv('CONFIG_S3_KEY', 'config.csv')
    notified_s3_bucket = 'yahoo--notifiedlist--2'
    notified_s3_key = 'notifiedlist.csv'
    batch_size = int(os.getenv('BATCH_SIZE', '100'))
    
    # S3クライアントの作成
    s3 = boto3.client('s3')
    
    # イベントから現在のバッチ番号を取得（デフォルトは0）
    current_batch = event.get('current_batch', 0)
    
    try:
        # S3からconfig.csvを取得
        response = s3.get_object(Bucket=config_s3_bucket, Key=config_s3_key)
        content = response['Body'].read().decode('utf-8')
        config_data = pd.read_csv(StringIO(content))
        print("config.csvをS3から正常に読み込みました。")
        # ここでconfig.csvの偶数行のみを対象にフィルタリング
        config_data = config_data.iloc[1::2]
    except Exception as e:
        print(f"S3からconfig.csvを読み込む際にエラーが発生しました: {e}")
        return {
            'statusCode': 500,
            'body': 'S3からconfig.csvを読み込む際にエラーが発生しました。'
        }
    
    # 総バッチ数を計算
    total_batches = (len(config_data) + batch_size - 1) // batch_size
    print(f"総バッチ数: {total_batches}")
    
    # 現在のバッチのデータを取得
    start_index = current_batch * batch_size
    end_index = start_index + batch_size
    batch_data = config_data.iloc[start_index:end_index]
    print(f"処理するバッチ番号: {current_batch + 1} / {total_batches} (行 {start_index + 1} から {min(end_index, len(config_data))} )")
    
    # 通知済みリストをS3から読み込む
    notified_set = load_notified_list(s3, notified_s3_bucket, notified_s3_key)
    
    # 商品を検索して通知
    updated_notified_set = search_and_notify(batch_data, application_id, chatwork_room_id, chatwork_api_token, notified_set)
    
    # 更新された通知済みリストをS3に保存する
    save_notified_list(s3, notified_s3_bucket, notified_s3_key, updated_notified_set)
    
    # 次のバッチが存在する場合は、次のバッチを処理するためにLambdaを再呼び出し
    if current_batch + 1 < total_batches:
        print("次のバッチを処理するためにLambdaを再呼び出します。")
        lambda_client = boto3.client('lambda')
        try:
            response = lambda_client.invoke(
                FunctionName=context.function_name,
                InvocationType='Event',  # 非同期呼び出し
                Payload=json.dumps({'current_batch': current_batch + 1})
            )
            print(f"次のバッチのLambda呼び出しステータス: {response['StatusCode']}")
        except Exception as e:
            print(f"次のバッチのLambda呼び出しに失敗しました: {e}")
            return {
                'statusCode': 500,
                'body': '次のバッチのLambda呼び出しに失敗しました。'
            }
    else:
        print("全てのバッチの処理が完了しました。")
    
    return {
        'statusCode': 200,
        'body': f'バッチ {current_batch + 1} の処理が正常に完了しました。'
    }