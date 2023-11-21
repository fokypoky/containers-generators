import psycopg2
import random
import pg_database_options as pgoptions
from psycopg2 import *
import datetime as dt
from redis import *
import redis_connection_options as rcon_opt
from neo4j import GraphDatabase
import pg_confr as pgc

people_names = []
# 5 групп на 1 кафедру. направление - основное на кафедре.
# номер группы - абрив. института + год - порядковый номер - год

current_year = str(dt.date.today().year % 100)
pg_connection = psycopg2.connect(
        host = pgoptions.host,
        user = pgoptions.user,
        password = pgoptions.password,
        database = pgoptions.db_name,
        port = pgoptions.port
)

#pg_connection = None

def insert_groups(department_id: int, institute_abr: str, speciality_id: int, last_iterator: int) -> None:
    groups = []
    for i in range(1, 6):
        group_name = f'{institute_abr}{current_year}-{last_iterator + i}-{current_year}'
        groups.append(group_name)

    with pg_connection.cursor() as cursor:
        for group in groups:
            print(group)
            query = f'''INSERT INTO groups(number, department_id, speciality_id)
                            VALUES ('{group}', '{department_id}', '{speciality_id}'); COMMIT;'''
            cursor.execute(query)
    print('rows inserted')


def shuffle_names(filepath:str) -> None:
    with open(filepath, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    random.shuffle(lines)
    with open(filepath, 'w', encoding='utf-8') as file:
        file.writelines(lines)


def read_names(filepath: str) -> []:
    with open(filepath, 'r', encoding='utf-8') as file:
        names = file.readlines()
    return names

handled_names = []

def handle_names():
    raw_names = read_names('names.txt')
    for name in raw_names:
        handled_names.append(name[:-1])

all_groups_ids = []

def fill_groups():
    with pg_connection.cursor() as cursor:
        query = '''SELECT id FROM groups;'''
        cursor.execute(query)
        for id in cursor:
            all_groups_ids.append(id[0])

def number_to_string(number: int) -> str:
    num_str = str(number)
    while len(num_str) < 4:
        num_str = '0' + num_str
    return num_str


# student: id, name, group_id, passbook_number
def insert_students():
    for group_id in all_groups_ids:
        for i in range(1, 31): # в каждой группе по 30 студентов
            with pg_connection.cursor() as cursor:
                student_name = random.choice(handled_names)
                student_passbook_number = str(group_id) + str(i) + str(current_year)
                query = f'''INSERT INTO students(name, group_id, passbook_number)
                            VALUES('{student_name}', {group_id}, '{student_passbook_number}'); 
                            COMMIT;'''
                cursor.execute(query)


def insert_students(group_id: int):
    for i in range(1, 31):
        with pg_connection.cursor() as cursor:
            student_name = random.choice(handled_names)
            student_passbook_number = str(group_id) + '0' + str(i) + str(current_year)
            query = f'''INSERT INTO students(name, group_id, passbook_number)
                            VALUES('{student_name}', {group_id}, '{student_passbook_number}'); 
                            COMMIT;'''
            cursor.execute(query)


def get_all_students_to_redis() -> []:
    with pg_connection.cursor() as cursor:
        query = '''
            SELECT s.passbook_number, s.name, g.number 
            FROM students s JOIN groups g
            ON s.group_id = g.id;
        '''
        cursor.execute(query)
        lst = []
        for row in cursor:
            lst.append(row)
        return lst

def insert_all_students_to_redis(students: []):
    redis = Redis(host=rcon_opt.host, port=rcon_opt.port, decode_responses=True)
    for student in students:
        key = student[0]
        value = f'{student[1]};{student[2]}'
        redis.set(f'{key}', f'{value}')
    print(f'inserted {len(students)} keys')

def get_by_key():
    redis = Redis(host=rcon_opt.host, port=rcon_opt.port, decode_responses=True)
    print(redis.get('190223'))

neo4j_driver = GraphDatabase.driver(uri='bolt://localhost:1933',auth=('neo4j','dr22042002'))

def make_neo4j_group_student_match_query(passbook_number: str, group_number: str) -> str:
    query = 'MATCH (s:Student{passbook_number: "' +  passbook_number + '"}), '
    query += '(g:Group{name: "' + group_number + '"}) '
    query += 'CREATE (s)-[:STUDY_IN]->(g);'
    return query

def insert_students_and_group_data_to_neo4j():
    groups = []
    with pg_connection.cursor() as cursor:
        query = '''select * from groups;'''
        cursor.execute(query)
        for data in cursor:
            groups.append(data)
    #id, number, department_id, speciality_id
    for group in groups:
        n4j_query = 'CREATE (g:Group{ name: "' + group[1] + '"});'
        neo4j_driver.execute_query(n4j_query)
        
        #id, name, group_id, passbook_number
        group_students = []
        
        with pg_connection.cursor() as cursor:
            query = f'''SELECT * FROM students WHERE group_id = {group[0]}'''
            cursor.execute(query)
            for student in cursor:
                group_students.append(student)

        for student in group_students:
            create_student_query = 'CREATE (s:Student{passbook_number: "' + student[3] + '"});'
            match_student_query = make_neo4j_group_student_match_query(student[3], group[1])
            neo4j_driver.execute_query(create_student_query)
            neo4j_driver.execute_query(match_student_query)
    print(f'inserted matches...')
            

def insert_all_departments_to_neo4j():
    departments = []
    with pg_connection.cursor() as cursor:
        query = '''select * from departments;'''
        cursor.execute(query)
        for department in cursor:
            departments.append(department)
    for department in departments:
        neo4j_query = 'CREATE (d:Department{name: "' + department[1] + '"});'
        neo4j_driver.execute_query(neo4j_query)


def match_departments_with_specs(department_id: int, specs: []):
    department_name = ""
    with pg_connection.cursor() as cursor:
        query = f'''SELECT title FROM departments WHERE id = {department_id}'''
        cursor.execute(query)
        for data in cursor:
            department_name = data[0]
    for spec in specs:
        neo4j_query = 'MATCH (d:Department{name: "'+ department_name +'"}), '
        neo4j_query += '(s:Speciality{code: "' + spec + '"}) '
        neo4j_query += 'CREATE (d)-[:PRODUCES]->(s);'
        neo4j_driver.execute_query(neo4j_query)
    print(f'department {department_name} added')

def insert_all_specs_to_neo4j():
    specs = []
    with pg_connection.cursor() as cursor:
        query = '''SELECT * FROM specialties;'''
        cursor.execute(query)
        for spec in cursor:
            specs.append(spec[2])
    for spec in specs:
        neo4j_query = 'CREATE (:Speciality{code: "' + spec + '"});'
        neo4j_driver.execute_query(neo4j_query)
