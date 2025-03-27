import yaml
import json
import csv

# maybe delete source files that already exist before running this!

def get_qtm_params():
    with open('config/qtm_source/qtm_params.json', 'r') as f:
        qtm_params = json.load(f)
    return qtm_params
    
def get_tables():
    """Read table names from the CSV file and return them as a list"""
    tables = []
    csv_file_path = 'config/qtm_source/qtm_tables_all.csv'
    
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row
        for row in csv_reader:
            if row:  # Ensure the row is not empty
                tables.append(row[0])  # Assuming table names are in the first column
    
    return tables


def get_schemas():
    """Read schema names from the CSV file and return them as a list"""
    schemas = []
    csv_file_path = 'config/qtm_source/qtm_schemas.csv'
    
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row
        for row in csv_reader:
            if row:  # Ensure the row is not empty
                schemas.append(row[0])  # Assuming schema names are in the first column
    
    return schemas


def generate_sources():
    """Generate the sources.yml content"""
    tables = get_tables()
    qtm_params = get_qtm_params()
    qtm_prefix = qtm_params['QTM_PREFIX']
    schemas = get_schemas()

    sources_config = {
        'version': 2,
        'sources': [
            {
                'name': schema,
                'database': 'cla_prod',
                'schema': qtm_prefix + schema,
                'loaded_at_field': '_fivetran_synced',  
                'freshness': {                          
                    'warn_after': {
                        'count': 24,
                        'period': 'hour'
                    }
                },                                      
                'tables': [
                    {
                        'name': table,
                        'identifier': table
                    }
                    for table in tables
                ]
            }
            for schema in schemas
        ]
    }
    
    return sources_config

def write_sources_file(output_path='models/qtm_union/sources.yml'):
    """Write the sources configuration to a YAML file"""
    sources_config = generate_sources()
    
    with open(output_path, 'w') as f:
        yaml.dump(sources_config, f, sort_keys=False, default_flow_style=False)

if __name__ == '__main__':
    write_sources_file()
    print("Sources file generated successfully!")