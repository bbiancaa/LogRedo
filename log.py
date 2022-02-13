import conn

def read_file(cursor):
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
                print('initial line: \t\t', line.replace(",", " ").replace("=", " ").split(" "))
                column, id, value = line.replace(",", " ").replace("=", " ").split(" ")
                cursor.execute("select * from at2")
                print('before update \t\t', cursor.fetchall())
                cursor.execute("update at2 set %s=%s where id=%s;" % (column, value, id))
                cursor.execute("select * from at2")
                print('after update \t\t', cursor.fetchall())

    return commands


def main():
    connection = conn.connect()
    cursor = connection.cursor()
    conn.create_table(cursor)

    commands = read_file(cursor)


if __name__ == '__main__':
    main()
