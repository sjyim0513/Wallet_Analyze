import csv
import sqlite3

# CSV 파일 경로
csv_file = 'export-EtherPrice.csv'

# SQLite 데이터베이스 경로
db_file = 'eth_prices.db'

# 데이터베이스 연결
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# 테이블 생성 쿼리
create_table_query = '''
    CREATE TABLE IF NOT EXISTS eth_prices (
        date TEXT PRIMARY KEY,
        price REAL
    )
'''
cursor.execute(create_table_query)

# 이미 저장된 데이터 조회
cursor.execute('SELECT date FROM eth_prices')
existing_dates = set(row[0] for row in cursor.fetchall())

# CSV 파일 데이터 저장
with open(csv_file, 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # 헤더 스킵

    # 새로운 데이터 저장
    new_data = []
    for row in csv_data:
        date = row[0]
        price = row[2]
        if date not in existing_dates:
            new_data.append((date, price))

    # 데이터베이스에 저장
    if new_data:
        cursor.executemany('INSERT INTO eth_prices VALUES (?, ?)', new_data)

# 변경 사항 커밋
conn.commit()

# 연결 종료
conn.close()
