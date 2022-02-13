import conn

def read_file():
    values = []
    commands = []
    with open("./entrada.txt", "r") as file:
        for line in file:
            line = line.replace("\n", "")
            line = line.strip()

            if line == "":
                continue

            if line.startswith("<") and line.endswith(">"):
                commands.append(line)
            else:
                values.append(line)
    return values, commands


def insert_values(cursor, values):
    for val in values:
        print('inicial line: \t\t', val.replace(",", " ").replace("=", " ").split(" "))
        column, id, value = val.replace(",", " ").replace("=", " ").split(" ")
        cursor.execute("select * from at2")
        print('before update \t\t', cursor.fetchall())
        cursor.execute("update at2 set %s=%s where id=%s;" % (column, value, id))
        cursor.execute("select * from at2")
        print('after update \t\t', cursor.fetchall())


def main():
    connection = conn.connect()
    cursor = connection.cursor()
    conn.create_table(cursor)

    values, commands = read_file()
    insert_values(cursor, values)


if __name__ == '__main__':
    main()
