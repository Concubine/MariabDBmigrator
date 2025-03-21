import mysql.connector
from mysql.connector import Error
import yaml

def load_config():
    """Load configuration from the config file"""
    try:
        with open('config/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
            return config
    except Exception as e:
        print(f"Error loading config: {str(e)}")
        return None

def test_connection():
    """Test connection to MariaDB/MySQL server"""
    config = load_config()
    if not config:
        return False
    
    db_config = config.get('database', {})
    
    # Print connection parameters
    print(f"Connecting to {db_config.get('host')}:{db_config.get('port')} as {db_config.get('user')}")
    print(f"Use pure: {db_config.get('use_pure', True)}")
    
    try:
        connection = mysql.connector.connect(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 3306),
            user=db_config.get('user', 'root'),
            password=db_config.get('password', ''),
            database=db_config.get('database', ''),
            use_pure=db_config.get('use_pure', True)
        )
        
        if connection.is_connected():
            print("Successfully connected to MariaDB/MySQL")
            server_info = connection.get_server_info()
            print(f"Server version: {server_info}")
            
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            print("Available databases:")
            for db in databases:
                print(f"- {db['Database']}")
            
            connection.close()
            return True
    except Error as e:
        print(f"Error connecting to MariaDB/MySQL: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection() 