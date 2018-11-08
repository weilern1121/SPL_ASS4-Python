import sqlite3
import os

import sys

workertask = dict()

def main(args):
    if os.path.isfile('world.db'):
        dbcon = sqlite3.connect('world.db')
    cur = dbcon.cursor()
    cur.execute("""
    SELECT *
    FROM tasks
    """)
    remaintasks = cur.fetchall()
    while len(remaintasks) > 0:
        for rtask in remaintasks:
            #   the person catch with the same task we need
            # print(workertask.get(rtask[0]) and workertask[rtask[2]] == rtask[0])
            if workertask.get(rtask[2]) and workertask[rtask[2]] == rtask[0]:
                # the task is over
                if time_dec(cur, dbcon, rtask[0]):
                    # release worker
                    Release_Person(dbcon, rtask[2])
                    # clean the dictionary
                    del workertask[rtask[2]]
                    # remove the task from DB
                    delete_task(dbcon, rtask[0])
                    workerName = get_worker_name(cur, rtask[2])
                    print("{} says: All Done!".format(workerName))
                else:
                    # the task not over yet
                    workerName = get_worker_name(cur, rtask[2])
                    taskName = get_task_name(cur, rtask[0])
                    print("{} is busy {}...".format(workerName, taskName))
            else:
                if Catch_Person(cur, dbcon, rtask[2]):
                    # the task succeed to catch the worker
                    new_amount = get_amount(cur, str(rtask[4]))
                    dec_amount = new_amount - rtask[5]
                    dbcon.execute("""UPDATE resources
                    SET amount=?
                    WHERE name like ?""", (str(dec_amount), str(rtask[4])))
                    workertask[rtask[2]] = rtask[0]
                    workerName = get_worker_name(cur, rtask[2])
                    print("{} says: work work".format(workerName))
        find_All_Done(cur,dbcon)
        cur = dbcon.cursor()
        cur.execute("""
        SELECT *
        FROM tasks
        """)

        remaintasks = cur.fetchall()
    dbcon.commit()
    dbcon.close()


def Catch_Person(cursor, dbcon, workerId):
    cursor.execute("""
    SELECT status 
    FROM workers
    WHERE id=? """, str(workerId))
    ans = cursor.fetchone()
    if str(ans[0]) == 'idle':
        dbcon.execute("""UPDATE workers
            SET status='busy'
            WHERE id=?
            """, str(workerId))
        return True
    else:
        return False


def Release_Person(cursor, workerId):
    cursor.execute("""UPDATE workers
            SET status='idle'
            WHERE id=?
            """, str(workerId))

def find_All_Done(cur,dbcon):
        cur.execute("""
        SELECT id, worker_id
        FROM tasks
        WHERE time_to_make=0
        """)
        remaintasks = cur.fetchall()
        for rtask in remaintasks:
            Release_Person(dbcon, rtask[1])
            # clean the dictionary
            del workertask[rtask[1]]
            # remove the task from DB
            delete_task(dbcon, rtask[0])
            workerName = get_worker_name(cur, rtask[1])
            print("{} says: All Done!".format(workerName))


def get_resource(dbcon, resourceName, resourceCapcity):
    dbcon.execute("""UPDATE resources
        SET amount=amount-?
        WHERE name=?""", (str(resourceCapcity), str(resourceName)))


def get_amount(cursor, res_name):
    cursor.execute("""SELECT amount
            FROM resources
            WHERE name like ? """, (str(res_name),))
    ans = cursor.fetchone()
    return ans[0]


# false=the task is not over yet  ,true= task is over
def time_dec(cursor, dbcon, task_id):
    cursor.execute("""SELECT time_to_make
            FROM tasks
            WHERE id=?""", str(task_id))
    ans = cursor.fetchone()
    if (ans[0] > 0):
        dbcon.execute("""UPDATE tasks
            SET time_to_make=time_to_make-1
            WHERE id=?""", str(task_id))
        return False
    else:
        return True


def delete_task(dbcon, task_id):
    dbcon.execute("""DELETE FROM tasks
        WHERE id=?""", str(task_id))


def get_worker_name(cursor, workerId):
    cursor.execute("""SELECT name
            FROM workers
            WHERE id=?""", str(workerId))
    ans = cursor.fetchone()
    return ans[0]


def get_task_name(cursor, task_Id):
    cursor.execute("""SELECT task_name
            FROM tasks
            WHERE id=(?)""", str(task_Id))
    ans = cursor.fetchone()
    return ans[0]


if __name__ == '__main__':
    main(sys.argv)
