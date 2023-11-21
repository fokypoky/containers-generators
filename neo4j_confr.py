from neo4j import GraphDatabase
import psycopg2
import pg_database_options as pgoptions

pg_connection = psycopg2.connect(
        host = pgoptions.host,
        user = pgoptions.user,
        password = pgoptions.password,
        database = pgoptions.db_name,
        port = pgoptions.port
)

neo4j_driver = GraphDatabase.driver(uri='bolt://localhost:1933',auth=('neo4j','dr22042002'))

def get_spec_id(code: str) -> int:
    with neo4j_driver.session() as session:
        q = f'MATCH(s:Speciality) where s.code="{code}" return id(s);'
        qresult = session.run(q)

        spec_node_id = -1

        for r in qresult:
            spec_node_id = r[0]
            break

        return spec_node_id

def get_course_id(course: str) -> int:
    with neo4j_driver.session() as session:
        q = f'MATCH(c:Course) where c.name ="' + course + '" return id(c);'
        qresult = session.run(q)
        node_id = -1
        for r in qresult:
            node_id = r[0]
            break
        return node_id

def get_group_id(group_name: str) -> int:
    with neo4j_driver.session() as session:
        q = f'MATCH(g:Group) where g.name="' + group_name + '" return id(g);'
        qresult = qresult = session.run(q)
        node_id = -1
        for r in qresult:
            node_id = r[0]
            break
        return node_id

def match_courses_with_spec(course_node_ids: [], spec_node_id: int):
    for course_node_id in course_node_ids:
        q = 'MATCH(c:Course),(s:Speciality) where id(c)=' + str(course_node_id) + ' and id(s)=' + str(spec_node_id) +' '
        q += 'create (s)-[:TEACHES]->(c);'
        result = neo4j_driver.execute_query(q)
        print(result)

def insert_all_courses():
    pg_courses = []
    with pg_connection.cursor() as cursor:
        q = "select title from courses"
        cursor.execute(q)
        for raw_data in cursor:
            pg_courses.append(raw_data[0])
    for pg_course in pg_courses:
        q = 'CREATE(c:Course{name: "' + pg_course +  '"});'
        neo4j_driver.execute_query(q)

def match_courses_with_specs():
    pg_specs = []
    with pg_connection.cursor() as cursor:
        q = 'select * from specialties'
        cursor.execute(q)
        for raw_data in cursor:
            pg_specs.append([raw_data[2], raw_data[1]]) # code, title
    print(len(pg_specs))
    for spec in pg_specs:
        with neo4j_driver.session() as session:
            print('-' * 25)
            spec_node_id = get_spec_id(spec[0])
            print(f'code: {spec[0]}, title: {spec[1]}, node id: {spec_node_id}')
            
            courses = []
            input_course = input('course name: ')
            while input_course != 'br':
                courses.append(input_course)
                input_course = input('course name: ')
            
            course_node_ids = []
            for course in courses:
                course_node_id = get_course_id(course)
                if course_node_id != -1:
                    course_node_ids.append(course_node_id)
                else:
                    print(f'error. cant find {course} node')
            if len(course_node_ids) > 0:
                match_courses_with_spec(course_node_ids, spec_node_id)
                print(f'{len(course_node_ids)} courses matched with {course}')


def insert_lectures_to_neo4j():
    pg_lectures=[]
    with pg_connection.cursor() as cursor:
        q = '''select id, annotation from lectures;'''
        cursor.execute(q)
        for raw_data in cursor:
            pg_lectures.append([raw_data[0], raw_data[1]])
    for lecture in pg_lectures:
        q = 'CREATE(:Lecture{pg_id: "' + str(lecture[0]) + '", annotation: "' + str(lecture[1]) +'"});'
        neo4j_driver.execute_query(q)
    print('ready')


def get_courses_by_speciality_ids(spec_code: str) -> []:
    with neo4j_driver.session() as session:
        q = 'MATCH (s:Speciality)-[:TEACHES]->(c:Course)  WHERE s.code="' + spec_code + '" return id(c);'
        qresult = session.run(q)
        return [record['id(c)'] for record in qresult.data()]

def get_department_id(department_name: str) -> int:
    with neo4j_driver.session() as session:
        q = f'MATCH(d:Department) where d.name ="' + department_name + '" return id(d);'
        qresult = session.run(q)
        node_id = -1
        for r in qresult:
            node_id = r[0]
            break
        return node_id
    
def match_group_with_courses(course_ids: [], group_name: str):
    for course_id in course_ids:
        q = 'MATCH(g:Group), (c:Course) where g.name="' + group_name + '" and id(c)=' + str(course_id) + ' '
        q += 'CREATE (g)-[:LEARNS]->(c);'
        neo4j_driver.execute_query(q)
# группы изучают те курсы, которые на их специальности
# получить кафедру -> получить список специльностей -> получить список курсов -> match
def match_groups_with_courses():
    pg_groups = [] # номер группы, код специальности
    with pg_connection.cursor() as cursor:
        q = 'select g.number, s.code from groups g join specialties s on g.speciality_id = s.id;'
        cursor.execute(q)
        for raw_data in cursor:
            pg_groups.append([raw_data[0], raw_data[1]])
    for group in pg_groups:
        print(group[0])
        group_name = group[0]
        spec_code = group[1]
        course_ids = get_courses_by_speciality_ids(spec_code)
        match_group_with_courses(course_ids, group_name)

# timetable - date, group_name, lecture_pg_id
def insert_timetable_to_neo4j():
    pg_timetable = []
    with pg_connection.cursor() as cursor:
        q = 'select t.date, g.number, t.lecture_id from timetable t join groups g on t.group_id = g.id;'
        cursor.execute(q)
        for raw_data in cursor:
            pg_timetable.append([raw_data[0], raw_data[1], raw_data[2]])  
    for tt in pg_timetable:
        print(f'{tt[0]}  {tt[1]}  {tt[2]}')
        q = 'CREATE(:Timetable{date: "' + str(tt[0]) + '", group_name: "' + str(tt[1]) +'", lecture_pg_id: "' + str(tt[2]) + '"});'
        neo4j_driver.execute_query(q)

def match_groups_and_timetable():
    pg_groups = [] # number
    with pg_connection.cursor() as cursor:
        q = 'select number from groups;'
        cursor.execute(q)
        for raw_data in cursor:
            pg_groups.append(raw_data[0])
    for group in pg_groups:
        q = 'MATCH(g:Group), (t:Timetable) where g.name="' + group + '" and t.group_name="' + group + '" create (g)-[:VISITS]->(t);'
        neo4j_driver.execute_query(q)


def match_lectures_with_timetable():
    lecture_ids = []
    with pg_connection.cursor() as cursor:
        q = '''select id from lectures;'''
        cursor.execute(q)
        for raw_data in cursor:
            lecture_ids.append(raw_data[0])
    for pg_lecture_id in lecture_ids:
        q = f'''MATCH(l:Lecture), (t:Timetable) where l.pg_id="{pg_lecture_id}" and t.lecture_pg_id="{pg_lecture_id}" create (l)-[:READS]->(t);'''
        neo4j_driver.execute_query(q)

match_lectures_with_timetable()