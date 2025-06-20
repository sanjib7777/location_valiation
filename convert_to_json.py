import os
import sqlparse
import re
import json

def safe_cast(val):
    val = val.strip()
    if val.startswith("'") and val.endswith("'"):
        return val.strip("'")
    try:
        return int(val)
    except ValueError:
        return val

def parse_insert_sql(sql_text):
    statements = sqlparse.split(sql_text)
    data = {}

    for stmt in statements:
        parsed = sqlparse.parse(stmt)[0]
        tokens = [t for t in parsed.tokens if not t.is_whitespace]

        if not (tokens[0].ttype is sqlparse.tokens.DML and tokens[0].value.upper() == "INSERT"):
            continue

        table_token = tokens[2]
        table_name = table_token.get_real_name() if hasattr(table_token, 'get_real_name') else str(table_token).strip()

        columns_match = re.search(r'\bINTO\b\s+\w+\s*\((.*?)\)', stmt, re.DOTALL | re.IGNORECASE)
        if not columns_match:
            continue
        columns = [col.strip() for col in columns_match.group(1).split(",")]

        values_match = re.findall(r'\(([^)]+)\)', stmt, re.DOTALL)
        rows = []
        for value_group in values_match:
            values = re.findall(r"'[^']*'|[^,]+", value_group)
            cleaned = [safe_cast(v) for v in values]

            if len(cleaned) == len(columns):
                rows.append(dict(zip(columns, cleaned)))

        data[table_name] = rows

    return data

if __name__ == "__main__":
    folder_path = "./sql_files"  # Change this to your folder containing .sql files
    json_output_folder = "./json_output"

    os.makedirs(json_output_folder, exist_ok=True)

    sql_files = [f for f in os.listdir(folder_path) if f.endswith(".sql")]

    for sql_file in sql_files:
        sql_path = os.path.join(folder_path, sql_file)
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_text = f.read()

        parsed_data = parse_insert_sql(sql_text)

        for collection_name, records in parsed_data.items():
            json_filename = f"{collection_name}.json"
            json_path = os.path.join(json_output_folder, json_filename)

            with open(json_path, "w", encoding="utf-8") as jf:
                json.dump(records, jf, ensure_ascii=False, indent=4)

            print(f"✅ Converted {sql_file} to {json_filename}")
