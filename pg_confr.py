import pg_database_options as pgoptions
import psycopg2
import random
from neo4j import GraphDatabase
from math import ceil

pg_connection = psycopg2.connect(
        host = pgoptions.host,
        user = pgoptions.user,
        password = pgoptions.password,
        database = pgoptions.db_name,
        port = pgoptions.port
)
neo4j_driver = GraphDatabase.driver(uri='bolt://localhost:1933',auth=('neo4j','dr22042002'))

def remove_dublicates_from_list(lst: []) -> []:
    return list(dict.fromkeys(lst))

def insert_courses( courses: []):
    for course in courses:
        with pg_connection.cursor() as cursor:
            course_hours = random.randrange(80, 120)
            query = f'''INSERT INTO courses(title, duration)
                        VALUES('{course}', {course_hours}); COM'''
            cursor.execute(query)

def read_courses_from_file(filepath: str) -> []:
    with open(filepath, 'r', encoding='utf-8') as file:
        courses = file.readlines()
    for i in range(0, len(courses)):
        course = courses[i][:-1]
        courses[i] = course
    return courses

#для каждого курса 1 семинар, 1 лекция, 2 лабы
def make_lectures():
    all_courses = []
    with pg_connection.cursor() as cursor:
        q = 'select id, title from courses;'
        cursor.execute(q)
        for raw_data in cursor:
            all_courses.append(raw_data)
    for course in all_courses:
        print('-' * 25)
        print(f'{course[1]}')
        s_anotation = input('seminar anotation: ')
        l_anotation = input('lecture anotation: ')
        lab_anotation = input('lab 1 anotation: ')
        slab_annotation = input('lab 2 anotation: ')
        with pg_connection.cursor() as cursor:
            q = f'''INSERT INTO lectures(annotation, type_id, course_id)
                    VALUES ('{s_anotation}', 1, {course[0]}),
                           ('{l_anotation}', 2, {course[0]}),
                           ('{lab_anotation}', 3, {course[0]}),
                           ('{slab_annotation}', 3, {course[0]});
                           COMMIT;'''
            cursor.execute(q)

def get_courses_from_neo4j_by_group(group_name: str) -> []:
    with neo4j_driver.session() as session:
        q = 'MATCH (g:Group)-[:LEARNS]->(c:Course) where g.name="' + group_name +'" return c;'
        qresult = session.run(q)
        courses = [record['c']['name'] for record in qresult.data()]
        return courses

def get_date(month: int, day: int, hour: int) -> str:
    m_str = f'0{month}' if month < 10 else str(month)
    d_str = f'0{day}' if day < 10 else str(day)
    h_str = f'0{hour}' if hour < 10 else str(hour)
    return f'2023.{m_str}.{d_str} {h_str}:00:00'

def make_timetable():
    all_groups = [] #id, name
    with pg_connection.cursor() as cursor:
        q = 'SELECT id, number FROM groups;'
        cursor.execute(q)
        for raw_data in cursor:
            all_groups.append([raw_data[0], raw_data[1]])
    
    all_count = []
    # получить список курсов из neo4j -> получить список лекций из postgres для каждой группы
    for group in all_groups:
        pg_group_id = group[0]
        group_name = group[1]
        
        # список курсов для этой группы
        group_courses = get_courses_from_neo4j_by_group(group_name)
        group_lecture_ids = []
        
        for course_name in group_courses:
            with pg_connection.cursor() as cursor:
                q = f"""SELECT l.id FROM lectures l JOIN courses c ON c.id = l.course_id
                    WHERE c.title='{course_name}';"""
                cursor.execute(q)
                for raw_data in cursor:
                    group_lecture_ids.append(raw_data[0])
        
        lectures_per_day = ceil(len(group_lecture_ids) / 14)
        lectures = []
        # генерация расписания на полгода
        for month in range(3, 10): # месяцы
            for day in range(1, 31):
                # количество пар в день
                current_day_lectures = [] # дата, id лекции
                #какие лекции будут в этот день
                
                for i in range(0, lectures_per_day):
                    lecture_id = random.choice(group_lecture_ids)
                    lecture_date = get_date(month, day, random.randrange(9, 21))
                    lectures.append([lecture_id, lecture_date])
        
        # print(lectures[0])
        # return
        for lecture in lectures:
            with pg_connection.cursor() as cursor:
                q = f"""INSERT INTO timetable(group_id, lecture_id, date)
                    VALUES({pg_group_id}, {lecture[0]}, '{lecture[1]}'); COMMIT;"""
                cursor.execute(q)
    

