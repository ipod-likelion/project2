import mysql.connector
import certifi
from dotenv import load_dotenv
import os

class DBConnection:
    load_dotenv()
    def connect(self):
        try:
            # TiDB
            # connection = mysql.connector.connect(
            #     host="gateway01.ap-northeast-1.prod.aws.tidbcloud.com",
            #     port=4000,
            #     user="uHFpB9rsHJdGBAM.root",
            #     password="AcD2HXDLxuiKURpx",
            #     database="test",
            #     ssl_ca=certifi.where(),  # 시스템에 맞는 최신 인증서 경로를 자동으로 지정
            #     ssl_verify_cert=True,
            #     ssl_verify_identity=True
            # )

            # cloud SQL
            connection = mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
                charset='utf8mb4',
                port=os.getenv("DB_PORT"),
                ssl_ca=certifi.where(),  # 시스템에 맞는 최신 인증서 경로를 자동으로 지정
                ssl_verify_cert=True,
                ssl_verify_identity=True
            )

            if connection.is_connected():
                print("Mysql 연결 성공!")
                return connection


        except mysql.connector.Error as e:
            print(f"연결 오류 발생: {e}")
