import psycopg2
from neo4j import GraphDatabase
import random
import math
import datetime

pg_connection = psycopg2.connect(
    host = 'localhost',
    user = 'fokypoky',
    password = 'toor',
    database = 'university',
    port = 10210
)

neo4j_driver = GraphDatabase.driver(uri='bolt://localhost:1933',auth=('neo4j','dr22042002'))

def get_pg_date(month: int, day: int, hour: int) -> str:
    month_str = f'0{month}' if month < 10 else f'{month}'
    day_str = f'0{day}' if day < 10 else f'{day}'
    hour_str = f'0{hour}' if hour < 10 else f'{hour}'
    return f'2023-{month_str}-{day_str} {hour_str}:00:00'

def execute_pg_query(query: str) -> None:
    with pg_connection.cursor() as cursor:
        cursor.execute(query + 'COMMIT;')

def get_all_courses_from_file() -> []:
    with open('course_titles.txt', 'r', encoding='utf-8') as file:
        return list(filter(lambda s: s.strip(), file.read().split('\n')))

def create_lecture(lecture: []) -> None: # lecture[0]= annotation, lecture[1]= reqs, lecture[2]= course_id, lecture[3]= type_id
    q = f"INSERT INTO lectures(annotation, requirements, course_id, type_id) VALUES('{lecture[0]}', '{lecture[1]}', {lecture[2]}, '{lecture[3]}');"
    execute_pg_query(q)

def get_students_by_group(group_id: int) -> []: # id, passbook number
    students = []
    with pg_connection.cursor() as cursor:
        q = f'''SELECT id, passbook_number FROM students WHERE group_id = {group_id};'''
        cursor.execute(q)
        for raw_data in cursor:
            students.append([raw_data[0], raw_data[1]])
    return students

def get_timetable_by_group(group_id: int) -> []: # lecture id
    with pg_connection.cursor() as cursor:
        q = f'''SELECT lecture_id, date from timetable WHERE group_id = {group_id}'''
        cursor.execute(q)
        timetable = []   
        for raw_data in cursor:
            timetable.append([raw_data[0], str(raw_data[1])])
        return timetable

def get_lecture_type_ids() -> []:
    types = []
    with pg_connection.cursor() as cursor:
        q = 'select id from lecture_types order by type;'
        cursor.execute(q)
        for rd in cursor:
            types.append(rd[0])
    return types

def get_all_courses_ids() -> []:
    courses = []
    with pg_connection.cursor() as cursor:
        q = 'SELECT id from courses;'
        cursor.execute(q)
        for rd in cursor:
            courses.append(rd[0])
    return courses

def get_annotations_from_file() -> []:
    with open('.\lecture_annotations.txt', 'r', encoding='utf-8') as file:
        return list(filter(lambda s: s.strip(), file.read().split('/////')))

def get_reqs_from_file() -> []:
    with open('.\lecture_req.txt', 'r', encoding='utf-8') as file:
        return list(filter(lambda s: s.strip(), file.read().split('/////')))

def get_group_lectures(group_number: str) -> []:
    result = []
    with neo4j_driver.session() as session:
        q = f'''MATCH(g:Group)-[l:LEARNS]->(c:Course) where g.name="{group_number}" return c.title;'''
        qresult = session.run(q)
        courses = [record['c.title'] for record in qresult.data()]
    
    result = []

    with pg_connection.cursor() as cursor:
        for course in courses:
            q = f"""SELECT l.id FROM lectures l JOIN courses c on l.course_id = c.id WHERE c.title = '{course}';"""
            cursor.execute(q)
            for raw_data in cursor:
                result.append(raw_data[0])

    return result

def get_all_groups() -> []:
    all_groups = [] # id, number, department title
    with pg_connection.cursor() as cursor:
        q = '''SELECT g.id, g.number, d.title FROM groups g JOIN departments d ON g.department_id = d.id;'''
        cursor.execute(q)
        for raw_data in cursor:
            all_groups.append([ raw_data[0], raw_data[1], raw_data[2] ])
    return all_groups

def get_names_from_file() -> []:
    with open('.\\names.txt', 'r', encoding='utf-8') as file:
        return file.read().split('\n')

def get_all_groups_ids() -> []:
    groups = []
    with pg_connection.cursor() as cursor:
        q = 'SELECT id FROM groups;'
        cursor.execute(q)
        for rd in cursor:
            groups.append(rd[0])
    return groups

def generate_students() -> None:
    groups = get_all_groups_ids()
    names = get_names_from_file()
    
    dt = datetime.datetime.now()
    index = int(f'{dt.minute}{dt.second}{dt.microsecond}')
    pg_insert_q = 'INSERT INTO students(name, group_id, passbook_number) VALUES'

    for group_id in groups:
        for i in range(0, 30):
            student_name = random.choice(names)
            passbook_number = index
            pg_insert_q += f"('{student_name}', {group_id}, '{passbook_number}'),"
            index += 1

    pg_insert_q = pg_insert_q[:-1] + ';'
    execute_pg_query(pg_insert_q)

def generate_courses() -> None:
    pg_insert_q = 'INSERT INTO courses(title, duration) VALUES'
    course_titles = get_all_courses_from_file()
    for course in course_titles:
        duration = random.randrange(80, 120)
        pg_insert_q += f"('{course}', {duration}),"
    pg_insert_q = pg_insert_q[:-1] + ';'
    execute_pg_query(pg_insert_q)

# WARNING: перед запуском, в neo4j группы должны быть связаны с курсами
def generate_timetable() -> None:
    all_groups = get_all_groups() # id, number, department title
    pg_insert_q = 'INSERT INTO timetable(group_id, lecture_id, date) VALUES'
    for month in range(5, 7):
        for group in all_groups:
            group_lectures = get_group_lectures(group[1]) # айдишники лекций
            group_timetable = {} # дата - ключ. позволит избежать ситуации, 
                                 #когда у группы 2 пары в одно и то же время
            group_lectures_per_day = math.ceil(len(group_lectures) / 7)

            for day in range(1,31):
                for i in range(0, group_lectures_per_day):
                    hour = random.randrange(9, 21)
                    date = get_pg_date(month, day, hour)
                    group_timetable[date] = random.choice(group_lectures)

            for date_key in group_timetable:
                pg_insert_q += f'''({group[0]}, {group_timetable[date_key]}, '{date_key}'),'''
    pg_insert_q = pg_insert_q[:-1] + ';'
    execute_pg_query(pg_insert_q)

# WARNING: должны быть сгенерированы курсы
def generate_lectures() -> None: # на каждый курс 1 лекция, 1 семинар, 2 лабы
    raw_annotation = get_annotations_from_file()
    annotations = []
    
    for annotation in raw_annotation:
        annotations.append(annotation.replace("'", ""))

    raw_reqs = get_reqs_from_file()
    reqs = []

    for req in raw_reqs:
        reqs.append(req.replace("'", ""))

    courses = get_all_courses_ids()

    execute_pg_query("INSERT INTO lecture_types(type) VALUES('Семинар'), ('Лекционное занятие'), ('Лабораторная работа');")
    
    lecture_type_ids = get_lecture_type_ids()
    lab_type_id = lecture_type_ids[0]
    lecture_type_id = lecture_type_ids[1]
    seminar_type_id = lecture_type_ids[2]

    for course_id in courses:
        rnd_annotations = random.sample(annotations, 4)
        for a in rnd_annotations:
            annotations.remove(a)
        
        lec = [rnd_annotations[0], random.choice(reqs), course_id, lecture_type_id]
        sem = [rnd_annotations[1], random.choice(reqs), course_id, seminar_type_id]
        lab1 = [rnd_annotations[2], random.choice(reqs), course_id, lab_type_id]
        lab2 = [rnd_annotations[3], random.choice(reqs), course_id, lab_type_id]

        create_lecture(lec)
        create_lecture(sem)
        create_lecture(lab1)
        create_lecture(lab2)

def generate_visits() -> None:
    all_groups = get_all_groups() #id, number, department title
    for group in all_groups:
        group_timetable = get_timetable_by_group(group[0])
        students = get_students_by_group(group[0]) #id, passbook number
        
        insert_query = 'INSERT INTO visits VALUES'

        for student in students:
            visits_count = random.randrange(0, len(group_timetable))
            student_visits = {} 
            for i in range(0, visits_count):
                visit = random.choice(group_timetable)
                student_visits[visit[1]] = visit[0]

            for date_key in student_visits:
                lecture_id = student_visits[date_key]
                insert_query += f"""({student[0]}, {lecture_id}, '{date_key}'),"""
        
        insert_query = insert_query[:-1] + ';'
        execute_pg_query(insert_query)