import psycopg2
from neo4j import GraphDatabase
import datetime as dt
import random

pg_connection = psycopg2.connect(
    host = 'localhost',
    user = 'fokypoky',
    password = 'toor',
    database = 'university',
    port = 10210
)

neo4j_driver = GraphDatabase.driver(uri='bolt://localhost:1933',auth=('neo4j','dr22042002'))

def get_students_by_group(group_id: int) -> []:
    students = []
    with pg_connection.cursor() as cursor:
        q = f'''SELECT passbook_number FROM students WHERE group_id = {group_id}'''
        cursor.execute(q)
        for rd in cursor:
            students.append(rd[0])
    return students

def get_pg_groups() -> []:
    groups = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT g.number, d.title FROM groups g JOIN departments d on g.department_id = d.id;'''
        cursor.execute(q)
        for rd in cursor:
            groups.append([rd[0], rd[1]])
    return groups

def get_pg_groups___() -> []:
    groups_id = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT id, number FROM groups;'''
        cursor.execute(q)
        for rd in cursor:
            groups_id.append([rd[0], rd[1]])
    return groups_id

def get_pg_courses() -> []:
    courses = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT title FROM courses'''
        cursor.execute(q)
        for rd in cursor:
            courses.append(rd[0])
    return courses

def get_pg_specialities() -> []:
    specialities = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT code FROM specialities;'''
        cursor.execute(q)
        for rd in cursor:
            specialities.append(rd[0])
    return specialities

def get_pg_departments() -> []: #d. title, main spec. code
    departments = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT d.title, s.code FROM departments d JOIN specialities s ON d.main_speciality_id = s.id;'''
        cursor.execute(q)
        for rd in cursor:
            departments.append([rd[0], rd[1]])
    return departments

def get_pg_lectures() -> []:
    lectures = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT id from lectures;'''
        cursor.execute(q)
        for rd in cursor:
            lectures.append(rd[0])
    return lectures

def get_pg_timetable() -> []:
    timetable = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT t.id, g.number, t.date, t.lecture_id FROM timetable t JOIN groups g on t.group_id = g.id'''
        cursor.execute(q)
        for rd in cursor:
            timetable.append([rd[0], rd[1], str(rd[2]), rd[3]])
    return timetable

def execute_neo4j_query(query: str) -> None:
    with neo4j_driver.session() as session:
        session.run(query)



def put_lectures_to_neo4j() -> None:
    pg_lectures = get_pg_lectures()
    n4_query = 'CREATE'
    for lecture_id in pg_lectures:
        n4_query += '(:Lecture{ pg_id: ' + str(lecture_id) + '}),'
    n4_query = n4_query[:-1] + ';'
    execute_neo4j_query(n4_query)

def put_timetable_to_neo4j() -> None:
    pg_timetable = get_pg_timetable() #group_id, lecture_id, date

    n4_insert_q = 'CREATE'
    counter = 0

    queries = []

    for tt in pg_timetable:
        n4_insert_q += '(:Timetable{pg_id: ' + str(tt[0]) + ', group_name: "' + tt[1] + '", date: "' + tt[2].replace(" ", "T") + "Z" + '", pg_lecture_id: ' + str(tt[3]) + '}),'
        counter += 1
        if(counter == 2000):
            n4_insert_q = n4_insert_q[:-1] + ';'
            queries.append(n4_insert_q)
            n4_insert_q = 'CREATE'
            counter = 0

    if n4_insert_q[-1] == ',':
        n4_insert_q = n4_insert_q[:-1] + ';'
        queries.append(n4_insert_q)

    for q in queries:
        with neo4j_driver.session() as session:
            session.run(q)
    
    n4_group_match_q = 'MATCH(t:Timetable), (g:Group) WHERE t.group_name = g.name CREATE (g)-[:VISITS]->(t);'
    n4_lecture_match_q = 'MATCH(t:Timetable), (l:Lecture) WHERE t.pg_lecture_id=l.pg_id CREATE (l)-[:READS]->(t)'

    execute_neo4j_query(n4_group_match_q)
    execute_neo4j_query(n4_lecture_match_q)


def put_departments_and_specialities_to_neo4j() -> None:
    departments = get_pg_departments() #d. title, main spec. code
    specialities = get_pg_specialities() # code
    
    for code in specialities:
        n4_q = 'CREATE(s:Speciality{code: "' + code + '"});'
        with neo4j_driver.session() as session:
            session.run(n4_q)
    
    for department in departments:
        n4_create_q = 'CREATE(d:Department{name: "' + department[0] + '"});'
        with neo4j_driver.session() as session:
            session.run(n4_create_q)

        department_specs = []
        department_specs.append(department[1])

        for i in range(0, 5):
            spec = random.choice(specialities)
            if spec not in department_specs:
                department_specs.append(spec)

        for ds in department_specs:
            n4_match_q = f'MATCH(d:Department), (s:Speciality) where d.name="{department[0]}" and s.code="{ds}" CREATE (d)-[:PRODUCES]->(s)'
            with neo4j_driver.session() as session:
                session.run(n4_match_q)        

def get_courses_by_department(department_title: str) -> []:
    with neo4j_driver.session() as session:
        q = f'MATCH(d:Department)-[:PRODUCES]->(s:Speciality)-[:TEACHS]->(c:Course) where d.name="{department_title}" return c;'
        qresult = session.run(q)
        return [res['c']['title'] for res in qresult.data()]

# 4 курса на специальнсть
# WARNING: узлы специальностей и групп должны быть уже созданы
def put_courses_to_neo4j() -> None:
    specialities = get_pg_specialities()
    courses = get_pg_courses()
    
    n4_create_q = 'CREATE'
    for course in courses:
        n4_create_q += '(:Course{title: "' + course +'"}),'
    
    n4_create_q = n4_create_q[:-1] + ';'
    with neo4j_driver.session() as session:
        session.run(n4_create_q)
    
    for code in specialities:
        spec_courses = random.sample(courses, 4)
        for course in spec_courses:
            n4_match_q = 'MATCH (s:Speciality), (c:Course) where s.code="'+ code +'" and c.title="'+ course +'" CREATE (s)-[:TEACHS]->(c);'
            with neo4j_driver.session() as session:
                session.run(n4_match_q)

    groups = get_pg_groups() # [номер группы, название кафедры]
    for group in groups:
        group_courses = get_courses_by_department(group[1])
        for course in group_courses:
            n4_match_q = f'MATCH (g:Group), (c:Course) where g.name="{group[0]}" and c.title="{course}" create (g)-[:LEARNS]->(c);'
            with neo4j_driver.session() as session:
                session.run(n4_match_q)

def put_group_and_students_to_neo4j() -> None:
    groups = get_pg_groups___()
    for group in groups:
        n4_group_create_q = 'CREATE(:Group{name: "'+ group[1] +'"});'
        with neo4j_driver.session() as session:
            session.run(n4_group_create_q)

        students = get_students_by_group(group[0])
        for student in students:
            n4_student_q = 'MATCH (g:Group) where g.name="'+ group[1] +'" '
            n4_student_q += 'CREATE (s:Student{passbook_number: "'+ student +'"}), (s)-[:STUDY_IN]->(g);'
            with neo4j_driver.session() as session:
                session.run(n4_student_q)