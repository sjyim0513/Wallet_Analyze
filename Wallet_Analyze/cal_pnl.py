import math
import sqlite3
import re
import decimal
import requests

RESERVED_KEYWORDS = ['PRIMARY', 'SELECT', 'INSERT', 'UPDATE', 'DELETE']

def is_reserved_keyword(symbol):
    return symbol.upper() in RESERVED_KEYWORDS

def create_summary_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS summary (
                        symbol TEXT,
                        realized_profit DECIMAL(18, 5),
                        realized_roi DECIMAL(18, 5),
                        unrealized_profit DECIMAL(18, 5),
                        trade_counts INTEGER,
                        avg_buy_price DECIMAL(18, 5),
                        avg_sell_price DECIMAL(18, 5),
                        total_profit DECIMAL(18, 5)
                    )''')
    conn.commit()

#테이블 가져와서 한번에 전체 이름 저장하게 -> url 매번 요청 안하게
def get_current_price(token_address=None):
    
    if token_address == None:
        return 0
    
    url = f"https://api.dexscreener.io/latest/dex/tokens/{token_address}"
    response = requests.get(url).json()
    if(response == None):
        print("Error: ", token_address)
        return 0
    
    pairs = response.get("pairs", [])
    if(pairs == None):
        print("Error: ", token_address)
        return 0
    
    price_usd = 0.0
    for pair in pairs:
        price_usd = float(pair.get("priceUsd"))
        break
    return price_usd


async def calculate_and_insert_summary(wallet_address, api_key=None):
    conn = sqlite3.connect(f"{wallet_address}.db")
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('summary', 'latest_block', 'token_info')")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        symbol = re.sub('[^a-zA-Z0-9]', '', table_name)
        if is_reserved_keyword(symbol):
            symbol = symbol + '_token'
    
        cursor.execute("SELECT * FROM {}".format(table_name))
        data = cursor.fetchall()
        #summary가 이미 있다면 다시 만들지 말고 데이터만 업데이트하게
        filtered_data = [item for item in data if decimal.Decimal(item[5]) != 0]

        if filtered_data:
            buy_quantity = sum([decimal.Decimal(item[1]) for item in data if not item[2]])
            print(f"처음 바이 총량 : {buy_quantity}")
            if buy_quantity == 0:
                cursor.execute('SELECT decimal, token_address FROM token_info WHERE symbol=(?)', (symbol,))
                decimal_, token_add = cursor.fetchone()
                print(type(decimal_), decimal_)
                print(f"산 기록이 없음 : {symbol}")
                print(f"토큰 주소 : {token_add}")
                url = f"https://eth-mainnet.g.alchemy.com/v2/{api_key}"
                token_amount = {
                                "jsonrpc": "2.0",
                                "id": 0,
                                "method": "alchemy_getTokenBalances",
                                "params": [
                                    "0xf2c06f90fb58844c09220e01e3116a2293df6960",
                                    [
                                    "0x0fe0ed7f146cb12e4b9759aff4fa8d34571802ca"
                                    ]
                                ]
                                }
                response = requests.post(url, json=token_amount)
                if response.status_code == 200:
                    data_ = response.json()
                    tokenBalance = data_['result']["tokenBalances"][0]["tokenBalance"]
                    if not isinstance(tokenBalance, int):
                        tokenBalance = 0
                    result = tokenBalance/math.pow(10, decimal_)
                    buy_quantity = result
                    print(f"현재 남은 balance : {buy_quantity}")

            sell_quantity = sum([decimal.Decimal(item[1]) for item in data if item[2]])
            print(f"판매량 : {sell_quantity}")
            total_dollars_spent = sum([decimal.Decimal(item[4]) * decimal.Decimal(item[5]) for item in data if not item[2]])
            total_dollars_received = sum(decimal.Decimal(item[4]) * decimal.Decimal(item[5]) for item in data if item[2])
            avg_buy_price = total_dollars_spent / buy_quantity if buy_quantity > 0 else 0
            avg_sell_price = total_dollars_received / sell_quantity if sell_quantity > 0 else 0
            realized_profit = (avg_sell_price - avg_buy_price) * (sell_quantity)    
            current_price_call = False
            #get token_address
            cursor.execute('SELECT token_address FROM token_info WHERE symbol=?', (data[0][0],))
            print(f"data(symbol) : {data[0][0]}")
            token_address = cursor.fetchone()
            print(f"token_add : {token_address}\n")
            if token_address == None:
                current_price = 0
                continue
            else:
                current_price = get_current_price(token_address[0])

            unrealized_profit = (current_price - float(avg_buy_price)) * float(sell_quantity)
            total_profit = float(realized_profit) + float(unrealized_profit)
            create_summary_table(conn)
            cursor.execute('''INSERT OR REPLACE INTO summary
                    (symbol, realized_profit, unrealized_profit, trade_counts, avg_buy_price, avg_sell_price, total_profit)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (str(symbol), str(realized_profit), str(unrealized_profit), len(data), str(avg_buy_price), str(avg_sell_price), str(total_profit)))

    conn.commit()

#구매량이 0일 때 남은 코인량 가져오는 코드 만들어야함


# 데이터베이스 연결
#conn = sqlite3.connect('0xf2c06f90fb58844c09220e01e3116a2293df6960.db')



# 각 토큰별로 실현 수익 계산 및 테이블에 삽입
#calculate_and_insert_summary(conn)

# 데이터베이스 연결 종료
#conn.close()
