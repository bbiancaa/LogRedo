from config import config
import psycopg2

def main():
    params = config()
    # conectando no banco
    conn = psycopg2.connect(**params)
    # instanciando objeto para consultas
    cur = conn.cursor()
    #realizando consulta
    cur.execute('SELECT * from redo')
    #pegando resultado 
    result = cur.fetchone()
    #printando resultado
    print(result)
    #fechando conexao com o banco
    cur.close()


if __name__ == '__main__':
    main()
