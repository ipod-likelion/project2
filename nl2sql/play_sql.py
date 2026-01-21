import pymysql
from db_connection import DBConnection

# DB 연결 정보
conn = DBConnection().connect()
# conn = pymysql.connect(host='localhost', user='root', password='password', db='your_db')
cursor = conn.cursor()
cursor.execute("SET SESSION sql_mode='ANSI_QUOTES';")

sql_command = ""
# 합쳐진 파일 읽기
with open('combined_all.sql', 'r', encoding='utf-8') as f:
    for line in f:
        # 주석이나 빈 줄은 건너뜀
        if line.startswith('--') or line.startswith('/*') or not line.strip():
            continue

        sql_command += line

        # 한 문장의 끝(;)을 찾음
        if line.rstrip().endswith(';'):
            cursor.execute(sql_command)

            try:
                sql_command = ""
                # 대량 INSERT 시 성능을 위해 일정 단위마다 commit 하거나
                # 혹은 매번 수행 (안정성)

            except conn.Error as e:
                print(f"❌ 개별 쿼리 오류: {e}")
                print(f"⚠️ 문제 발생 쿼리: {sql_command[:100]}...")
                continue  # 오류 발생 시 다음 문장으로 진행할지 여부 결정

    conn.commit()
    print("✅ 모든 SQL 문이 성공적으로 반영되었습니다.")