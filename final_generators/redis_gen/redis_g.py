import psycopg2
import redis

pg_connection = psycopg2.connect(
    host = 'localhost',
    user = 'fokypoky',
    password = 'toor',
    database = 'university',
    port = 10210
)

def get_all_students() -> []:
    students = []
    with pg_connection.cursor() as cursor:
        q = 'SELECT s.name, g.number, s.passbook_number FROM students s JOIN groups g ON s.group_id = g.id;'
        cursor.execute(q)
        for rd in cursor:
            students.append([rd[2], f'{rd[0]};{rd[1]}'])
    return students

def insert() -> None:
    students = get_all_students()
    r = redis.Redis(host='localhost', port=13700, db=0)
    for student in students:
        r.set(f'{student[0]}', f'{student[1]}')