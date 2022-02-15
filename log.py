import conn


def beautiful_str(str, value):
    return str.replace('<', '').replace('>', '').replace(value, '').strip()


def process_commands(cursor, values):
    controller = {

    }
    redo = {

    }
    for key, value in enumerate(values):
        if 'Start CKPT' in value:
            for trasiction in beautiful_str(value, 'Start CKPT(').replace(')', '').split(','):
                redo[trasiction] = False
            for start_ckpt in values[key+1:]:
                if 'Start CKPT' in start_ckpt:
                    break
                elif 'commit' in start_ckpt:
                    position = beautiful_str(start_ckpt, 'commit')
                    for commit in reversed(controller[position]):
                        id, column, val = commit.split(',')
                        cursor.execute(f'select {column} from at2 where id={id}')
                        result = cursor.fetchall()
                        if result[0][0] != val:
                            redo[position] = True
                            cursor.execute(f"update at2 set {column}={val} where id={id};")
                        controller[position].pop()
                elif 'End CKPT' in start_ckpt:
                    continue 
                elif 'crash' in start_ckpt:
                    return redo
                elif 'start' in start_ckpt:
                    redo[beautiful_str(start_ckpt, 'start')] = False
                    controller[beautiful_str(start_ckpt, 'start')] = []
                else:
                    position = beautiful_str(start_ckpt, '').split(',')[0]
                    controller[position].insert(0, beautiful_str(start_ckpt, position+','))

        elif 'start' in value:
            controller[beautiful_str(value, 'start')] = []
        elif 'commit' in value:
            position = beautiful_str(value, 'commit')
            for commit in reversed(controller[position]):
                id, column, val = commit.split(',')
                cursor.execute(f"update at2 set {column}={val} where id={id};")
                controller[position].pop()
        elif 'crash' in value:
            return redo
        elif 'End CKPT' in value:
            continue 
        else:
            position = beautiful_str(value, '').split(',')[0]
            controller[position].insert(0, beautiful_str(value, position+','))

    return redo

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
                column, id, value = line.replace(",", " ").replace("=", " ").split(" ")
                cursor.execute("update at2 set %s=%s where id=%s;" % (column, value, id))

    return commands


def main():
    connection = conn.connect()
    cursor = connection.cursor()
    conn.create_table(cursor)

    commands = read_file(cursor)
    redos = process_commands(cursor, commands)
    for ckpt in redos:
        if redos[ckpt]:
            print(f'Transação {ckpt} realizou Redo')
            continue
        print(f'Transação {ckpt} não realizou Redo')
    cursor.execute("select * from at2")
    for val in reversed(cursor.fetchall()):
        print(f"ID: {val[0]}\t A: {val[1]}\t B: {val[2]}")


if __name__ == '__main__':
    main()
