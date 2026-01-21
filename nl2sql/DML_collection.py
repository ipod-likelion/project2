import os

# 1. SQL 파일들이 들어있는 최상위 폴더 경로
input_folder = '/Users/young/Downloads/nia/database/'
# 2. 결과물이 저장될 파일명
output_file = 'combined_all_v1.sql'


def combine_sql_files(src_folder, dest_file):
    with open(dest_file, 'w', encoding='utf-8') as outfile:
        # os.walk를 사용해 하위 폴더까지 모두 탐색
        for root, dirs, files in os.walk(src_folder):
            for filename in files:
                # 확장자가 .sql인 파일만 대상
                if filename.endswith('.sql'):
                    file_path = os.path.join(root, filename)

                    # 어떤 파일이 합쳐지고 있는지 확인용 출력
                    print(f"Merging: {file_path}")

                    # 파일 구분용 주석 추가 (선택 사항)
                    outfile.write(f"\n\n-- Source: {file_path}\n")

                    # 실제 SQL 내용 읽어서 쓰기
                    with open(file_path, 'r', encoding='utf-8') as infile:
                        outfile.write(infile.read())

    print(f"\n✅ 모든 파일이 '{dest_file}'로 합쳐졌습니다.")


# 실행
combine_sql_files(input_folder, output_file)