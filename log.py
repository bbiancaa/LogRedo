import conn


def beautiful_str(str, line):
    return str.replace('<', '').replace('>', '').replace(line, '').strip()


def process_commands(cursor, lines):
    controller = {

    }
    redo = {

    }
    for key, line in enumerate(lines):
        if 'Start CKPT' in line:
            for trasiction in beautiful_str(line, 'Start CKPT(').replace(')', '').split(','):
                redo[trasiction] = False
            for start_ckpt in lines[key+1:]:
                if 'Start CKPT' in start_ckpt:
                    break
                elif 'commit' in start_ckpt:
                    id_transaction = beautiful_str(start_ckpt, 'commit')
                    for commit in reversed(controller[id_transaction]):
                        id, column, val = commit.split(',')
                        cursor.execute(f'select {column} from at2 where id={id}')
                        result = cursor.fetchall()
                        if result[0][0] != val:
                            redo[id_transaction] = True
                            cursor.execute(f"update at2 set {column}={val} where id={id};")
                        controller[id_transaction].pop()
                elif 'End CKPT' in start_ckpt:
                    continue 
                elif 'crash' in start_ckpt:
                    return redo
                elif 'start' in start_ckpt:
                    redo[beautiful_str(start_ckpt, 'start')] = False
                    controller[beautiful_str(start_ckpt, 'start')] = []
                else:
                    id_transaction = beautiful_str(start_ckpt, '').split(',')[0]
                    controller[id_transaction].insert(0, beautiful_str(start_ckpt, id_transaction+','))

        elif 'start' in line:
            controller[beautiful_str(line, 'start')] = [] 
        elif 'commit' in line:
            id_transaction = beautiful_str(line, 'commit') 
            for commit in reversed(controller[id_transaction]): 
                id, column, val = commit.split(',')
                cursor.execute(f"update at2 set {column}={val} where id={id};")
                controller[id_transaction].pop()
        elif 'crash' in line:
            return redo
        elif 'End CKPT' in line:
            continue 
        else:
            id_transaction = beautiful_str(line, '').split(',')[0]
            controller[id_transaction].insert(0, beautiful_str(line, id_transaction+','))

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
                column, id, line = line.replace(",", " ").replace("=", " ").split(" ")
                cursor.execute("update at2 set %s=%s where id=%s;" % (column, line, id))

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
