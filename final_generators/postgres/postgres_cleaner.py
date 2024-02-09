import psycopg2
pg_connection = psycopg2.connect(
    host = 'localhost',
    user = 'fokypoky',
    password = 'toor',
    database = 'university',
    port = 10210
)
def execute_pg_query(query: str) -> None:
    with pg_connection.cursor() as cursor:
        cursor.execute(query + 'COMMIT;')

def delete() -> None:
    execute_pg_query('DELETE FROM lecture_materials;')
    execute_pg_query('DELETE FROM timetable;')
    execute_pg_query('DELETE FROM visits;')
    execute_pg_query('DELETE FROM students;')
    execute_pg_query('DELETE FROM lectures;')
    execute_pg_query('DELETE FROM lecture_types;')
    execute_pg_query('DELETE FROM courses;')
    execute_pg_query('DELETE FROM groups;')
    execute_pg_query('DELETE FROM departments;')
    execute_pg_query('DELETE FROM specialities;')