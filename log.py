import psycopg2
import conn


def main():
    connection = conn.connect()
    cursor = connection.cursor()
    conn.create_table(cursor)

if __name__ == '__main__':
    main()
