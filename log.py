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
    return (values, commands)

def main():
    connection = conn.connect()
    cursor = connection.cursor()
    conn.create_table(cursor)

    values, commands = read_file()
    print(values, commands)


if __name__ == '__main__':
    main()
