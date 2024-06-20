import sqlite3
import re
import asyncio
import aiohttp
import itertools
import os
import json
from functools import wraps
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
from web3 import Web3
import cal_pnl

load_dotenv()

app = Flask(__name__)
CORS(app)

api_key_ = os.getenv("API_KEY")
router_addresses = os.getenv("ROUTER_ADDRESS").split(",")
w3_url = os.getenv("W3_URL")
db_file = os.getenv("DB_FILE")

RESERVED_KEYWORDS = ['PRIMARY', 'SELECT', 'INSERT', 'UPDATE', 'DELETE']
w3 = Web3(Web3.HTTPProvider(w3_url))

def async_action(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        coro = f(*args, **kwargs)
        return asyncio.run(coro)
    return wrapped

@app.route('/api/address', methods=['POST'])
@async_action
async def post_address():
    data = request.get_json()
    address = data['address']
    response = await start_transfer(api_key_, address, router_addresses)
    return jsonify(response)

async def get_eth_price(timestamp):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # 주어진 timestamp를 날짜 형식으로 변환
    date_object = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    month = date_object.strftime('%m').lstrip('0')  # 월 (앞의 0 제거)
    day = date_object.strftime('%d').lstrip('0')  # 일 (앞의 0 제거)
    year = date_object.strftime('%Y')  # 연도
    date = f'{month}/{day}/{year}'  # 날짜 형식 변환

    # 데이터베이스에서 해당 날짜에 대한 가격 정보 조회
    cursor.execute('SELECT price FROM eth_prices WHERE date = ?', (date,))
    result = cursor.fetchone()

    # 연결 종료
    cursor.close()

    # 조회 결과 반환
    if result:
        return result[0]  # 첫 번째 열의 값 (가격)
    else:
        return None

async def get_token_symbol(token_address, api_key):
    url = f"https://eth-mainnet.alchemyapi.io/v2/{api_key}"
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getTokenMetadata",
        "params": [token_address]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            data = await response.json()


    if "result" in data:
        decimals = data['result'].get('decimals')
        symbol = data['result'].get('symbol')
        if decimals is not None and symbol is not None:
            return decimals, symbol

    return None, None

async def get_transaction(api_key, escan_api_key, log, transaction_hash, wallet_address, timestamp, to):
    
    print(f"죽여줘 얏팔 : {to}")
    data = log
   # print(transaction_hash)
    if len(data) < 2: #단순 토큰 전송
        return []

    trace_data = []
    token_sell_address = ""
    token_buy_address = ""
    token_sell_amount = 0
    token_buy_amount = 0
    buy_symbol = ""
    sell_symbol = ""
    eth_amount = 0
    eth_price = 0
    for item in data: #sell 토큰 추출
        topics = item['topics']
        if len(topics) < 3 or len(topics) > 3: #토픽의 수가 3개인 경우만 취급
            continue
        check_value = topics[0].hex()
        check_value_for_sell = topics[1].hex()[26:] # 판매인지 구별하기 위한 토픽 추출
        
        if(check_value == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and check_value_for_sell == wallet_address[2:].lower()):
            token_sell_address = item['address'].lower()         
            sell_decimal_s, sell_symbol_s = await get_token_symbol(token_sell_address, api_key)
            if sell_decimal_s is None or sell_symbol is None:
                return []
            
            token_sell_amount = int(item['data'].hex(), 16) / 10**sell_decimal_s
            sell_symbol = sell_symbol_s
            sell_decimal = sell_decimal_s
            break
    
    for item in data: # buy토큰 추출
        topics = item['topics']
        if len(topics) < 3 or len(topics) > 3: #토픽의 수가 3개인 경우만 취급
            continue
        check_value = topics[0].hex()
        check_value_for_buy = topics[2].hex()[26:] # 판매인지 구별하기 위한 토픽 추출

        if(check_value == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and check_value_for_buy == wallet_address[2:].lower()):
            token_buy_address = item['address'].lower()
            buy_decimal_s, buy_symbol_s = await get_token_symbol(token_buy_address, api_key)
            
            if buy_decimal_s is None or buy_symbol is None:
                #print(transaction_hash)
                return []
            token_buy_amount = int(item['data'].hex(), 16) / 10**buy_decimal_s
            buy_symbol = buy_symbol_s
            buy_decimal = buy_decimal_s
            break

    # WETH amount 계산
    linear_check_value_2 = ""
    for item in data:
        topics = item['topics']
        address = item['address'].lower()
        if(len(topics) < 3 or len(topics) > 3):
            continue
        check_value = topics[0].hex()
        check_linear_value = topics[1].hex()[26:]
        if(check_linear_value == linear_check_value_2):
            continue
        if(check_value == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and address == "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()):
            linear_check_value_2 = topics[2].hex()[26:]
            eth_amount += int(item['data'].hex(), 16) / 10**18


    eth_price = await get_eth_price(timestamp)
    if eth_price is None:
                url = f"https://api.etherscan.io/api?module=stats&action=ethprice&apikey={escan_api_key}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response_eth_price:
                        result_eth_price = await response_eth_price.json()

                result_eth_price = result_eth_price.get("result", {}).get("ethusd", [])
                if result_eth_price:
                    eth_price = float(result_eth_price[0])
                else:
                    eth_price = None  # 또는 다른 값으로 처리
            

    if(token_sell_address and token_buy_address): # 판매와 구매가 동시에 일어난 경우
        #print(transaction_hash)
        trace_data.extend([
            {
                'token_address': token_sell_address,
                'symbol': sell_symbol,
                'token_amount': token_sell_amount,
                'isSell': True,
                'hash': transaction_hash,
                'eth_price': eth_price,
                'eth_amount': eth_amount,
                'decimal' : sell_decimal,
                'timestamp' : timestamp,
                'router_address' : to
            },
            {   
                'token_address': token_buy_address,
                'symbol': buy_symbol,
                'token_amount': token_buy_amount,
                'isSell': False,
                'hash': transaction_hash,
                'eth_price': eth_price,
                'eth_amount': eth_amount,
                'decimal' : buy_decimal,
                'timestamp' : timestamp,
                'router_address' : to
            }
        ])   
    elif(token_buy_address != ""):
        trace_data.append({
                'token_address': token_buy_address,
                'symbol': buy_symbol,
                'token_amount': token_buy_amount,
                'isSell': False,
                'hash': transaction_hash,
                'eth_price': eth_price,
                'eth_amount': eth_amount,
                'decimal' : buy_decimal,
                'timestamp' : timestamp,
                'router_address' : to
        })
    else:
        trace_data.append({
                'token_address': token_sell_address,
                'symbol': sell_symbol,
                'token_amount': token_sell_amount,
                'isSell': True,
                'hash': transaction_hash,
                'eth_price': eth_price,
                'eth_amount': eth_amount,
                'decimal' : sell_decimal,
                'timestamp' : timestamp,
                'router_address' : to
        })
    #print(trace_data)
    
    #token address 저장(decimal 추출용)
    
    return trace_data
    
async def fetch_data(session, url, payload):
    async with session.post(url, json=payload) as response:
        result = await response.json()
        transfers = result.get("result", {}).get("transfers", [])
        return transfers

 #summary 내용 db에서 추출           

def get_info_from_summary(wallet_address):
    conn = sqlite3.connect(f"{wallet_address}.db")
    cursor_address = conn.cursor()
    cursor_address.execute('SELECT symbol, realized_profit, realized_roi, unrealized_profit FROM summary'.format(wallet_address))
    result = cursor_address.fetchall()
    
    data = []
    for row in result:
        item ={
            'symbol' : row[0],
            'realized_profit' : row[1],
            'realized_roi' : row[2],
            'unrealized_profit' : row[3]
        }
        data.append(item)
        
    json_data = json.dumps(data)
    
    return json_data

#block이 현재와 db가 같은지 체크 / return은 json형식
def match_block(wallet_address, current):
    print(wallet_address)
    print("~~~~~~~~~~")
    
    
    if os.path.exists(f'{wallet_address}.db'): #이미 사용자가 있어
        conn_address = sqlite3.connect(f'{wallet_address}.db')
        cursor_address = conn_address.cursor()
        cursor_address.execute("SELECT * FROM latest_block")
         
        db_block = cursor_address.fetchone()
    
        if db_block == current : #같은 블럭 내에 검색함
            print("Block Matched! Same block")
            conn_address.close
            return get_info_from_summary(wallet_address)
        else : #서로 다를 경우 db에 저장된 블럭
            print("Block diff!") 
            conn_address.close
            return int(db_block[0])
    
        
    return

#처음 실행되는 놈
async def start_transfer(api_key, wallet_address, router_addresses):
    current_block_url = os.getenv('CURRENT_BLOCK_NUMBER')
    escan_api_key = os.getenv("ESCAN_API_KEY")

    current_block_payload = {
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": escan_api_key,
        "format": "json"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(current_block_url, params=current_block_payload) as response:
            current_block_response = await response.json()
        #현재 블록 저장
        current_block_number = int(current_block_response["result"], 16)
        
    block_result = match_block(wallet_address, current_block_number)
    default_search_block = 50 * 24 * 60 * 60 // 15
    print(f"현재 블록 : {current_block_number}")
    if block_result == None: #새로운 사용자   
        print("block : NULL")   
        result = await get_asset_transfers(wallet_address, router_addresses, escan_api_key, api_key, default_search_block, current_block_number)
        return result
    elif isinstance(block_result, int): #블럭값 차이를 찾아서 그만큼 실행해야함
        print("block less then current")
        result = await get_asset_transfers(wallet_address, router_addresses, escan_api_key, api_key, block_result, current_block_number)
        return result
    else : #같은 블럭 내에서 검색 -> 수정할 것 없음
        print("block same as latest")
        return block_result
 
#transfer 정보 검색 
async def get_asset_transfers(wallet_address, router_addresses, escan_api_key, api_key, block_from, current_block_number):   
    url = f"https://eth-mainnet.alchemyapi.io/v2/{api_key}"
    
    #가져올 블럭 수 / default는 130일 정도
    if block_from > 17000000: #db에 값이 있는 경우
        from_block_number = block_from
        print(f"블럭 디비에 있음 : {from_block_number}")
    else:
        from_block_number = current_block_number - block_from
        print(f"디폴트 블록 : {block_from}")
    print(f"가져올 블록 : {from_block_number}")
    
    transfers = [] 
    tasks = []
    async with aiohttp.ClientSession() as session:
        for router_address in router_addresses:
            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [
                    {
                        "fromBlock": hex(from_block_number),
                        "toBlock": "latest",
                        "fromAddress": wallet_address,
                        "toAddress": router_address,
                        "category": ["external"],
                        "withMetadata": True,
                        "excludeZeroValue": False,
                        "maxCount": hex(1000)  # 예시로 최대 1000개의 트랜잭션을 가져옴
                    }
                ]
            }
            print(f"이게 찐라우터 : {router_address}")
            tasks.append(fetch_data(session, url, payload))
        for task in asyncio.as_completed(tasks):
            result = await task
            transfers.extend(result)
    
    #가져올 transaction이 없으면 summary 리턴
    print(f"transfer len : {len(transfers)}")
    if len(transfers) == 0 :
        return get_info_from_summary(wallet_address) #계산, 출력 분해해서 더 빠르게
    
    trace_data = []
    coroutines = []
    batch_size = 100  # Define the batch size
    num_batches = (len(transfers) + batch_size - 1) // batch_size  # Calculate the number of batches
    for i in range(num_batches):
        batch = transfers[i * batch_size : (i + 1) * batch_size]  # Get the transfers for the current batch

        coroutines = []
        for item in batch:
            # Process each transfer within the batch
            hash = item['hash']
            timestamp = item['metadata']['blockTimestamp']
            
            receipt = w3.eth.get_transaction_receipt(hash)
            #print(f"레시피 : {receipt}\n")
            logs = receipt.logs
            to = receipt.to
            print("어우 머리가 뜨끈하다\n")
            coroutines.append(get_transaction(api_key, escan_api_key, logs, hash, wallet_address, timestamp, to))
        # Wait for the coroutines within the batch to complete
        if coroutines:
            trace_data += await asyncio.gather(*coroutines)
    
    #print(trace_data)
    trace_data = [item for item in trace_data if item]  # None 제거
    print(type(trace_data))

    
    #print(trace_data)
    data = list(itertools.chain.from_iterable(trace_data))
 
    save_data_to_database(wallet_address, data, current_block_number)
    
    await calculate_pnl(wallet_address, api_key)
    
    return get_info_from_summary(wallet_address)
    
def is_reserved_keyword(symbol):
    return symbol.upper() in RESERVED_KEYWORDS

def create_table(conn, table_name=None):
    cursor = conn.cursor()
    
    #create transaction table
    if table_name != None:
        cursor.execute('''CREATE TABLE IF NOT EXISTS {} (
                            symbol TEXT,
                            token_amount REAL,
                            isSell BOOLEAN,
                            hash TEXT PRIMARY KEY,
                            eth_price REAL,
                            eth_amount REAL,
                            timestamp TEXT,
                            router_address TEXT,
                            FOREIGN KEY (symbol) REFERENCES token_info(symbol)
                        )'''.format(table_name))
        return
    
    #create token_info table
    cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name="token_info"''')
    symbol_table = cursor.fetchone()
    
    if symbol_table == None:
        cursor.execute('''CREATE TABLE IF NOT EXISTS token_info (
                            symbol TEXT PRIMARY KEY,
                            token_address TEXT,
                            decimal SMALLINT
                        )''')
    
    #create latest_block table
    cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name="latest_block"''')
    block_table = cursor.fetchone()
    
    if block_table == None:
        cursor.execute('''CREATE TABLE IF NOT EXISTS latest_block (
                            block SMALLINT
                        )''')
    
    conn.commit()

async def calculate_pnl(wallet_address, api_key):    
    await cal_pnl.calculate_and_insert_summary(wallet_address, api_key)

def insert_or_update_data(conn, data, current_block):
    cursor = conn.cursor()
    print(f"insert에서 current : {current_block}")
    print(f"추가할 데이터 길이 : {len(data)}")

    create_table(conn)
    
    if data != None:
        for item in data:
            symbol = item['symbol']
            token_address = item['token_address']
            token_amount = float(item['token_amount'])  # Convert to float
            isSell = item['isSell']
            hash = item['hash']
            eth_price = float(item['eth_price'])
            eth_amount = float(item['eth_amount'])
            table_name = re.sub('[^a-zA-Z0-9]', '', symbol)
            decimal = item['decimal']
            timestamp = item['timestamp']
            router_address = item['router_address']
            
            if is_reserved_keyword(table_name):
                table_name = table_name + '_token'
            
            cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name=?''',(table_name,))
            db_table_name = cursor.fetchone()
            
            #insert data if it's new
            cursor.execute('''SELECT symbol FROM token_info WHERE symbol=?''', (symbol,))
            is_symbol_exist = cursor.fetchone()
            print(f"테이블 만들 때 심볼 : {is_symbol_exist}")
            if is_symbol_exist == None: #doesn't exist
                cursor.execute('''INSERT OR IGNORE INTO token_info 
                (symbol, token_address, decimal) 
                VALUES (?, ?, ?)''', (table_name, token_address, decimal))
            
            if table_name == db_table_name:
                print(f"테이블 내용 업데이트 : {table_name}")
                cursor.execute('''INSERT OR IGNORE INTO {}
                        (symbol token_amount, isSell, hash, eth_price, eth_amount, timestamp, router_address)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''.format(table_name),
                        (table_name, token_amount, isSell, hash, eth_price, eth_amount, timestamp, router_address))
            else: # Create table if it doesn't exist
                create_table(conn, table_name)
                cursor.execute('''INSERT OR IGNORE INTO {}
                        (symbol, token_amount, isSell, hash, eth_price, eth_amount, timestamp, router_address)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''.format(table_name),
                        (table_name, token_amount, isSell, hash, eth_price, eth_amount, timestamp, router_address))
    
    #updata current block height
    if current_block != None:
        cursor.execute("SELECT * FROM latest_block")
        is_block = cursor.fetchone()
        
        if is_block != None:
            cursor.execute('''UPDATE latest_block SET block = ?''', (current_block,))
        else:
            cursor.execute('''INSERT OR IGNORE INTO latest_block (
                            block
                        ) VALUES (?)''', (current_block,))        
    
    conn.commit()

def save_data_to_database(wallet_address, data, current_block):
    # 데이터베이스 경로
    db_file = f'{wallet_address}.db'

    # 데이터베이스 연결
    conn = sqlite3.connect(db_file)

    
    # 데이터 저장
    insert_or_update_data(conn, data, current_block)

    # 연결 종료
    conn.close()

app.run()   

#시작
#if __name__ == '__main__':
#   app.start_transfer(host='0.0.0.0', port=5000)