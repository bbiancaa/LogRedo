import psycopg2

PG_DB = 'bd2'
PG_USER = 'postgres'
PG_PSWD = 'postgres'
PG_HOST = 'localhost'
PG_PORT = 5432


def connect():
    connection = psycopg2.connect(
        database=PG_DB,
        user=PG_USER,
        password=PG_PSWD,
        host=PG_HOST,
        port=PG_PORT,
    )
    return connection

def create_table(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS at2 (
            Id INT PRIMARY KEY,
            A INT,
            B INT
        );
    ''')

    cursor.execute("delete from at2;")
    cursor.execute("insert into at2 values (1, null, null);")
    cursor.execute("insert into at2 values (2, null, null);")
    cursor.execute("select * from at2;")
