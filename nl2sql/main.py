import pandas as pd
from flask import Flask, request, jsonify
from db_connection import DBConnection
import sqlparse

app = Flask(__name__)
db = DBConnection()


def get_table_names(sql):
    parsed = sqlparse.parse(sql)[0]
    table_names = []

    extract = False
    for token in parsed.tokens:
        if token.is_keyword and token.value.upper() in ('FROM', 'JOIN'):
            extract = True
        elif extract:
            if not token.is_whitespace:
                table_names.append(str(token))
                extract = False
    return [t.split()[0].replace('"', '').replace('`', '').strip() for t in table_names if t]


def compare_queries(query_1, query_2):
    report = {
        "status": "success",
        "metrics": {},
        "is_perfect": False
    }
    try:
        conn = db.connect()
        if not conn:
            raise Exception("DB Connection Failed")

        # 1. 테이블명 추출
        tables_1 = get_table_names(query_1)
        tables_2 = get_table_names(query_2)

        # 2. SQL 실행
        df1 = pd.read_sql(query_1, conn)
        df2 = pd.read_sql(query_2, conn)

        rows_a, cols_a = df1.shape
        rows_b, cols_b = df2.shape

        # 소문자로 통일하여 비교 (대소문자 구분 없이 이름만 맞으면 될 경우)
        cols_name_a = [str(c).lower() for c in df1.columns]
        cols_name_b = [str(c).lower() for c in df2.columns]

        # 순서까지 똑같아야 한다면 == 비교, 순서 상관없이 이름만 다 있으면 된다면 set 비교
        col_names_match = cols_name_a == cols_name_b

        # 3. 데이터 내용 일치도 (정렬 후 비교)
        # 컬럼명이 같아야만 데이터를 안전하게 비교할 수 있으므로 조건부 실행
        data_match = False
        if col_names_match:
            df1_sorted = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
            df2_sorted = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)
            data_match = df1_sorted.equals(df2_sorted)

        # 테이블명 일치 여부
        table_match = set(tables_1) == set(tables_2)

        # 4. 리포트 데이터 정제
        report["metrics"] = {
            "data_content_match": data_match,
            "idx_1_table_row_match": table_match and (rows_a == rows_b),
            "idx_2_table_row_col_match": table_match and (rows_a == rows_b) and (cols_a == cols_b)
        }
        report["is_perfect"] = data_match and table_match and (cols_a == cols_b)

        return report
    except Exception as e:
        return {"status": "error", "error_msg": str(e), "is_perfect": False}


@app.route('/nl2sql/confirm/list', methods=['POST'])
def confirm_queries():
    data = request.json  # [{"query_a": "..", "query_b": ".."}, ...]
    if not data:
        return jsonify({"error": "No data"}), 400

    final_results = []
    total_count = len(data)

    # 통계용 변수
    idx1_correct_count = 0  # 테이블명 & Row수 일치 개수
    idx2_correct_count = 0  # 테이블명 & Row수 & 컬럼수 일치 개수
    perfect_match_count = 0  # 완전 일치 개수

    for pair in data:
        q_a = pair.get('query_a')
        q_b = pair.get('query_b')

        comparison = compare_queries(q_a, q_b)

        # 통계 카운트
        if comparison.get("status") == "success":
            metrics = comparison["metrics"]
            if metrics["idx_1_table_row_match"]: idx1_correct_count += 1
            if metrics["idx_2_table_row_col_match"]: idx2_correct_count += 1
            if comparison["is_perfect"]: perfect_match_count += 1

        final_results.append({
            "pair": pair,
            "result": comparison
        })

    # 최종 응답 데이터 정제 (통계 포함)
    response = {
        "summary": {
            "total_queries": total_count,
            "indicator_1": {
                "correct_count": idx1_correct_count,
                "accuracy": round(idx1_correct_count / total_count * 100, 2) if total_count > 0 else 0
            },
            "indicator_2": {
                "correct_count": idx2_correct_count,
                "accuracy": round(idx2_correct_count / total_count * 100, 2) if total_count > 0 else 0
            },
            "perfect_match": {
                "correct_count": perfect_match_count,
                "accuracy": round(perfect_match_count / total_count * 100, 2) if total_count > 0 else 0
            }
        },
        "details": final_results
    }

    return jsonify(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)