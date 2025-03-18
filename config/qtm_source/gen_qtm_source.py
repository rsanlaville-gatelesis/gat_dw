import snowflake.connector
import yaml

# Snowflake connection parameters
conn_params = {
    'user': 'REMYSANLAVILLE',
    'password': 'GATelesis001!',
    'account': 'qr93446.east-us-2.azure',
    'warehouse': 'COMPUTE_WH',
    'database': 'cla_prod'
}

QTM_PREFIX = 'quantum_prod_'

def get_tables():
    """Get list of tables from Snowflake"""
    conn = snowflake.connector.connect(**conn_params)
    cursor = conn.cursor()
    
    try:
        query = """
        select distinct lower(table_name) table_name
        from cla_prod.information_schema.tables
        where table_schema = 'QUANTUM_PROD_QCTL' 
        and table_type = 'BASE TABLE'
        and table_name not like '%FIVETRAN%'
        """
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    finally:
        cursor.close()
        conn.close()

def generate_sources():
    """Generate the sources.yml content"""
    tables = get_tables()
    schemas = ['qctl', 'gatcrgqctl', 'gatcrgse', 'gatrs']
    
    sources_config = {
        'version': 2,
        'sources': [
            {
                'name': schema,
                'database': 'cla_prod',
                'schema': QTM_PREFIX + schema,
                'loaded_at_field': '_fivetran_synced',  # Added comma
                'freshness': {                          # Fixed indentation
                    'warn_after': {
                        'count': 24,
                        'period': 'hour'
                    }
                },                                      # Added comma
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

def write_sources_file(output_path='sources.yml'):
    """Write the sources configuration to a YAML file"""
    sources_config = generate_sources()
    
    with open(output_path, 'w') as f:
        yaml.dump(sources_config, f, sort_keys=False, default_flow_style=False)

if __name__ == '__main__':
    write_sources_file()
    print("Sources file generated successfully!")