import argparse
import os
import shutil
import json
from process_sql import get_sql

# NIA 데이터셋의 다양한 폴더명 변주를 처리하기 위한 목록
SOURCE_KEYWORDS = ["원천데이터", "01.원천데이터", "1.원천데이터", "TS"]
LABEL_KEYWORDS = ["라벨링데이터", "02.라벨링데이터", "2.라벨링데이터", "TL"]

class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, schema, table):
        self._schema = schema
        self._table = table
        self._idMap = self._map(self._schema, self._table)

    @property
    def schema(self):
        return self._schema

    @property
    def idMap(self):
        return self._idMap

    def _map(self, schema, table):
        column_names_original = table['column_names_original']
        table_names_original = table['table_names_original']
        for i, (tab_id, col) in enumerate(column_names_original):
            if tab_id == -1:
                idMap = {'*': i}
            else:
                key = table_names_original[tab_id].lower()
                val = col.lower()
                idMap[key + "." + val] = i

        for i, tab in enumerate(table_names_original):
            key = tab.lower()
            idMap[key] = i

        return idMap

def get_schemas_from_json(fpath):
    with open(fpath, encoding='utf-8') as f:
        data = json.load(f)
    db_names = [db['db_id'] for db in data]

    tables = {}
    schemas = {}
    for db in data:
        db_id = db['db_id']
        schema = {} 
        column_names_original = db['column_names_original']
        table_names_original = db['table_names_original']
        tables[db_id] = {'column_names_original': column_names_original,
                         'table_names_original': table_names_original}
        for i, tabn in enumerate(table_names_original):
            table = str(tabn.lower())
            cols = [str(col.lower()) for td, col in column_names_original if td == i]
            schema[table] = cols
        schemas[db_id] = schema

    return schemas, db_names, tables

def find_target_folder(root_dir, keywords):
    """키워드가 포함된 폴더를 찾습니다."""
    for item in os.listdir(root_dir):
        if any(k in item for k in keywords) and os.path.isdir(os.path.join(root_dir, item)):
            return os.path.join(root_dir, item)
    return None

def get_all_files(root_path):
    """폴더 깊이에 상관없이 모든 파일을 찾아냅니다 (os.walk 사용)"""
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield root, file

def load_json_file(json_file_path, _type="list"):
    if os.path.exists(json_file_path):
        try:
            with open(json_file_path, encoding='utf-8') as rfile:
                json_data = json.load(rfile)
        except Exception as e:
            print(f"Error reading {json_file_path}: {e}")
            json_data = [] if _type == "list" else {}
    else:
        json_data = [] if _type == "list" else {}
    return json_data

def table_check(data):
    # (기존 로직 유지)
    column_origin_dict = dict()
    column_dict = dict()
    table_dict = dict()

    if type(data["table_names_original"]) == str:
        data["table_names_original"] = [data["table_names_original"]]
    if type(data["table_names"]) == str:
        data["table_names"] = [data["table_names"]]

    column_origin_names = data["column_names_original"]
    column_names = data["column_names"]
    column_types = data["column_types"]
    col_names = []

    for i, col in enumerate(column_names):
        col_name = col[1]
        col_origin_name = column_origin_names[i][1]
        col_names.append(col_name)

        if col_name not in column_dict:
            column_dict[col_name] = 1
        else:
            column_dict[col_name] += 1

    if len(column_types) != len(column_origin_names):
        return False

    col_names = list(set(col_names))
    col_names.sort()
    col_count = len(column_dict)

    if col_count > 50:
        return False

    return True

def get_labeled_data(schemas, tables, db_id_list, data):
    # (기존 로직 유지)
    db_id = data["db_id"]
    if db_id in db_id_list:
        try:
            spider_format = {
                "db_id": str(), "utterance_id": str(), "hardness": str(),
                "utterance_type": str(), "query": str(), "query_toks": list(),
                "query_toks_no_value": list(), "question": str(), "question_toks": list(),
                "values": list(), "cols": list(), "sql": dict()
            }
            sql = data["query"]
            schema = schemas[db_id]
            table = tables[db_id]
            schema = Schema(schema, table)
            sql_label = get_sql(schema, sql)
            
            spider_format["db_id"] = db_id
            spider_format["utterance_id"] = data["utterance_id"]
            spider_format["hardness"] = data["hardness"]
            spider_format["utterance_type"] = data["utterance_type"]
            spider_format["query"] = sql
            spider_format["question"] = data["utterance"]
            spider_format["question_toks"] = data["utterance"].split()
            spider_format["sql"] = sql_label

            gold_data = sql + "\t" + db_id + "\n"
            return spider_format, gold_data
        except Exception as e:
            return False, False
    else:
        return False, False

def main(config):
    labeling_json_path = os.path.join(config.data_path, config.name + ".json")
    annotation_json_path = os.path.join(config.data_path, "tables.json")
    
    # 기존 tables.json이 있으면 로드
    annotation_list = load_json_file(annotation_json_path)
    labeling_list = []
    labeled_list = []
    db_id_list = [annotation_data["db_id"] for annotation_data in annotation_list]

    os.makedirs(os.path.join(config.data_path, config.database_path), exist_ok=True)
    
    print(f"Scanning folder: {config.src_folder}")
    
    # 1. 원천데이터(Source) 폴더 찾기
    source_root = find_target_folder(config.src_folder, SOURCE_KEYWORDS)
    if source_root:
        print(f"Found Source folder: {source_root}")
        for parent_path, file_name in get_all_files(source_root):
            src_path = os.path.join(parent_path, file_name)
            
            # JSON 파일이면 -> 스키마 정보(tables.json)로 추가
            if file_name.endswith(".json"):
                target_dict = load_json_file(src_path, _type="dict")
                if "data" in target_dict:
                    for annotation_data in target_dict["data"]:
                        db_id = annotation_data["db_id"]
                        if db_id not in db_id_list and table_check(annotation_data):
                            annotation_list.append(annotation_data)
                            db_id_list.append(db_id)
            
            # SQL/SQLite 파일이면 -> database 폴더로 복사
            elif file_name.endswith(".sqlite") or file_name.endswith(".sql"):
                dst_path = os.path.join(config.data_path, config.database_path, file_name)
                # print(f"Copying DB: {file_name}")
                if not os.path.exists(dst_path):
                    shutil.copy2(src_path, dst_path)

    # 2. 라벨링데이터(Label) 폴더 찾기
    label_root = find_target_folder(config.src_folder, LABEL_KEYWORDS)
    if label_root:
        print(f"Found Label folder: {label_root}")
        for parent_path, file_name in get_all_files(label_root):
            src_path = os.path.join(parent_path, file_name)
            
            if file_name.endswith(".json"):
                target_dict = load_json_file(src_path, _type="dict")
                if "data" in target_dict:
                    labeling_list.extend(target_dict["data"])

    # 저장 로직
    print(f"# of tables collected: {len(annotation_list)}")
    with open(annotation_json_path, 'w', encoding='utf-8') as wf:
        json.dump(annotation_list, wf, indent=4, ensure_ascii=False)

    schemas, db_names, tables = get_schemas_from_json(annotation_json_path)

    gold_data_list = []
    except_count = 0
    
    print("Processing Labeling Data...")
    for data in labeling_list:
        labeled_data, gold_data = get_labeled_data(schemas, tables, db_id_list, data)
        if labeled_data:
            labeled_list.append(labeled_data)
            gold_data_list.append(gold_data)
        else:
            except_count += 1

    print(f"Saving {config.name}.json...")
    with open(labeling_json_path, 'w', encoding='utf-8') as wf:
        json.dump(labeled_list, wf, indent=4, ensure_ascii=False)

    with open(labeling_json_path.split(".json")[0] + "_gold.sql", 'wt', encoding='utf-8') as out:
        out.writelines(gold_data_list)

    print(f"# Completed! (Failed items: {except_count})")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--src_folder", default="download/01.Training", type=str, help="data_folder")
    parser.add_argument("--name", choices=["train", "valid", "dev"], type=str, help="set name")
    parser.add_argument("--data_path", default="nia", type=str, help="data folder name")
    parser.add_argument("--database_path", default="database", type=str, help="database folder")
    args = parser.parse_args()
    main(args)