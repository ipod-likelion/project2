import re

input_file = "combined_all_v1.sql"
output_file = "combined_all_v9.sql"

# 외래 키 에러가 났던 컬럼들 리스트
target_columns = ["NAMES_OF_GOODS", "MINERAL_NAME"]

with open(input_file, 'r', encoding='utf-8', errors='replace') as f, \
        open(output_file, 'w', encoding='utf-8') as out:
    # 1. 파일 최상단 설정 (가장 강력한 조합)
    out.write("SET NAMES utf8mb4;\n")
    out.write("SET CHARACTER SET utf8mb4;\n")
    out.write("SET SESSION sql_mode='ANSI_QUOTES';\n")
    out.write("SET FOREIGN_KEY_CHECKS = 0;\n")
    out.write("SET UNIQUE_CHECKS = 0;\n\n")

    for line in f:
        # 1. 외래 키 구문 제거
        line = re.sub(r",\s*FOREIGN KEY.*REFERENCES.*(?=\s*\))", "", line, flags=re.IGNORECASE)

        # 2. 데이터 타입 수정
        line = line.replace(" INT,", " BIGINT,").replace(" INT ", " BIGINT ")
        for col in target_columns:
            line = line.replace(f'"{col}" TEXT', f'"{col}" VARCHAR(255)')

        # 3. 홑따옴표 처리 (한글 포함 버전으로 업그레이드)
        if "VALUES" in line.upper():
            # [핵심 수정] 알파벳, 숫자뿐만 아니라 '한글' 사이에 낀 따옴표도 ''로 치환
            # 예: '시점'이' -> '시점''이'
            line = re.sub(r"(?<=[a-zA-Z0-9가-힣])'(?=[a-zA-Z0-9가-힣])", "''", line)

            # [추가] 공백이나 특수문자 뒤에 오는 따옴표가 문장의 끝이 아닌 경우 처리
            # 예: '제안서 ' 제출' -> '제안서 '' 제출'
            # 단, ', ' (데이터 구분자)는 건드리지 않도록 주의해야 함
            # 아래는 단어 중간의 따옴표를 더 포괄적으로 잡음
            # line = re.sub(r"(\S)'(\S)", r"\1''\2", line)

        # 4. 줄바꿈 처리 (10000자 -> 1000자로 대폭 축소하여 문장 잘림 방지)
        # Cloud SQL 버퍼 에러를 피하기 위해 더 자주 줄을 바꿔줍니다.
        if len(line) > 1000:
            line = line.replace("), (", "),\n(")
            line = line.replace("), (", "),\n(")  # 한 번 더 확실하게

        out.write(line)

    out.write("\nSET FOREIGN_KEY_CHECKS = 1;\n")


print(f"✅ 변환 완료! v8 파일을 Cloud SQL에 업로드하세요.")