import os
import csv
import json



def get_tables():
    with open('.config/qtm_union/qtm_tables_all.csv') as csvfile:
        data = list(csv.reader(csvfile))
        data = [item for sublist in data for item in sublist]
        data = data[1:]
    return data

def generate_model_file(table_name):
    content = f"{{{{ generate_qtm_union_model('{table_name}') }}}}"
    
    filename = f"models/qtm_union/vw_{table_name}_qtm_union.sql"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w') as f:
        f.write(content)

def main():
    tables = get_tables()
    for table in tables:
        generate_model_file(table)

if __name__ == "__main__":
    main()