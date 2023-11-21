import random
import psycopg2
import pg_database_options as pgoptions
from neo4j import GraphDatabase
import requests

pg_connection = psycopg2.connect(
        host = pgoptions.host,
        user = pgoptions.user,
        password = pgoptions.password,
        database = pgoptions.db_name,
        port = pgoptions.port
)
neo4j_driver = GraphDatabase.driver(uri='bolt://localhost:1933',auth=('neo4j','dr22042002'))
# с 3 по 9 месяц
elastic_url = 'http://26.172.236.37:9200/courses/_search'
elastic_headers = {'Content-Type': 'application/json'}

def get_partition_table_name(month: int) -> str:
    return f'visits0{month}2023' if month < 10 else f'visits{month}2023'

def generate_partitions():
    for month in range(3, 10):
        table_name = get_partition_table_name(month)
        with pg_connection.cursor() as cursor:
            q = f"""CREATE TABLE {table_name}(LIKE visits INCLUDING ALL); COMMIT;"""
            cursor.execute(q)

def get_lecture_ids_by_group(group_number: str, month: int) -> []:
    with neo4j_driver.session() as session:
        q = f''' MATCH (t:Timetable) WHERE toInteger(SPLIT(t.date, '-')[1]) = {month}
                and t.group_name="{group_number}"
                return t; 
        '''
        qresult = session.run(q)
        result = [d['t'] for d in qresult.data()]
        return result
        

def get_students_ids_by_group(group_name):
    students_passbooks =[]
    with pg_connection.cursor() as cursor:
        q = f'''select s.id from groups g join students s on g.id = s.group_id 
            where g.number='{group_name}';'''
        cursor.execute(q)
        for raw_data in cursor:
            students_passbooks.append(raw_data[0])
    return students_passbooks

def generate_visits():
    group_numbers = []
    with pg_connection.cursor() as cursor:
        q = '''select number from groups;'''
        cursor.execute(q)
        for raw_data in cursor:
            group_numbers.append(raw_data[0])
    for month in range(3, 10):
        visits_table_name = get_partition_table_name(month)
        for group_number in group_numbers:
            group_month_lectures = get_lecture_ids_by_group(group_number, month)
            group_students = get_students_ids_by_group(group_number)
            for sudent_id in group_students:
                lectures_count = random.randrange(0, len(group_month_lectures))
                if lectures_count == 0:
                    continue
                student_lectures = [] # список лекций которые он посетит
                for i in range(0, lectures_count):
                    random_lecture = random.choice(group_month_lectures)
                    if random_lecture not in student_lectures:
                        student_lectures.append(random_lecture)
                        continue
                    i -= 1
                for lecture in student_lectures:
                    date = lecture['date']
                    lecture_pg_id = lecture['lecture_pg_id']
                    with pg_connection.cursor() as cursor:
                        q = f'''insert into {visits_table_name}(student_id, lecture_id, date) 
                            values ({sudent_id}, {lecture_pg_id}, '{date}'); COMMIT;'''
                        cursor.execute(q)
ids = []
def match_courses_with_description_id():
    courses = [] # id, title
    with pg_connection.cursor() as cursor:
        q = '''select id, title from courses;'''
        cursor.execute(q)
        for raw_data in cursor:
            courses.append([raw_data[0], raw_data[1]])
    
    for course in courses:
        q = {
            "query": {
                "match_phrase": {
                    "title": course[1]
                }
            }
        }
        response = requests.get(url=elastic_url, headers=elastic_headers,json=q)
        json = response.json()
        elastic_id = json['hits']['hits'][0]['_id']
        with pg_connection.cursor() as cursor:
            q = f"""update courses set description_id = '{elastic_id}' where id = {course[0]}; COMMIT;"""
            cursor.execute(q)

def put_course_titles_to_txt() -> None:
    titles = []
    with pg_connection.cursor() as cursor:
        q = '''select title from courses;'''
        cursor.execute(q)
        for raw_data in cursor:
            titles.append(raw_data[0])
    with open('course_titles.txt', 'a', encoding='utf-8') as file:
        for title in titles:
            file.write(f'{title}\n')