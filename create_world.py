import sqlite3
import os
import sys


def main(args):
    databaseexisted = os.path.isfile('world.db')
    dbcon = sqlite3.connect('world.db')
    dbcon.text_factory = bytes
    with dbcon:
        cur = dbcon.cursor()
        if not databaseexisted:
            inputfilename = args[1]
            if not os.path.isfile(inputfilename):
                return

            # create tables if not already existed
            cur.execute("""
            CREATE TABLE tasks(
            id INTEGER PRIMARY KEY,
            task_name TEXT NOT NULL,
            worker_id INTEGER REFERENCES workers(id),
            time_to_make INTEGER NOT NULL,
            resource_name TEXT NOT NULL,
            resource_amount INTEGER NOT NULL
            );""")
            cur.execute("""
            CREATE TABLE workers(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL
            );""")
            cur.execute("""
            CREATE TABLE resources (
            name TEXT PRIMARY KEY,
            amount INTEGER NOT NULL
            ); """)

    with open(inputfilename) as inputfile:
        index = 0
        for row in inputfile:
            row = row.rstrip('\n')
            row = row.rstrip(' ')
            data_array = row.split(',')

            if data_array[0] == "worker":
                cur.execute("""
                INSERT INTO workers VALUES (?,?,?)""", (data_array[1], data_array[2], "idle"))
                index += 1

            elif len(data_array) == 2:
                cur.execute("""
                INSERT INTO resources VALUES(?,?)""", (data_array[0], data_array[1]))
                index += 1

            # the only other option is task line ->larger then 2
            else:
                cur.execute("""
                INSERT INTO tasks VALUES (?,?,?,?,?,?)""",
                            (index, data_array[0], data_array[1], data_array[4], data_array[2], data_array[3]))
                index += 1

    dbcon.commit()
    dbcon.close()


if __name__ == '__main__':
    main(sys.argv)
