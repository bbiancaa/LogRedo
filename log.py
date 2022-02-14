import conn


def beautiful_str(str, value):
    return str.replace('<', '').replace('>', '').replace(value, '').strip()


def process_commands(cursor, values):
    start = {

    }
    checkpoint = {

    }
    for value in values:
        if 'Start CKPT' in value:
            checkpoint[beautiful_str(value, 'Start CKPT')] = []
        elif 'start' in value:
            start[beautiful_str(value, 'start')] = []
        elif 'commit' in value:
            position = beautiful_str(value, 'commit')
            for commit in reversed(start[position]):
                id, column, val = commit.split(',')
                cursor.execute(f"update at2 set {column}={val} where id={id};")
                start[position].pop(0)

        elif 'End CKPT' in value:
            pass
        elif 'crash' in value:
            pass
        else:
            position = beautiful_str(value, '').split(',')[0]
            start[position].append(beautiful_str(value, position+','))

    print(start, checkpoint)
    
def read_file(cursor):
    commands = []
    with open("./entrada.txt", "r") as file:
        for line in file:
            line = line.replace("\n", "").strip()

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
    process_commands(cursor, commands)


if __name__ == '__main__':
    main()
