import psycopg2
import pg_database_options as pgoptions
import requests
import random

pg_connection = psycopg2.connect(
        host = pgoptions.host,
        user = pgoptions.user,
        password = pgoptions.password,
        database = pgoptions.db_name,
        port = pgoptions.port
)

url = 'http://localhost:10103/courses/_doc'
headers = {'Content-Type': 'application/json'}

def get_courses() -> []: #id, title
    courses = [] 
    with pg_connection.cursor() as cursor:
        q = '''select id, title from courses;'''
        cursor.execute(q)
        for raw_data in cursor:
            courses.append([raw_data[0], raw_data[1]])
    return courses

def get_descriptions() -> []: #id, description
    with open('./elastic/courses_description.txt', 'r', encoding='utf-8') as file:
        return list(filter(lambda s: s.strip(), file.read().split('/////')))

elastic_failed_titles = []
elastic_inds = []

def put_description_to_elastic(description: str, course_title: str) -> str:
    document = {
        'title': course_title,
        'content': description
    }
    response = requests.post(url=url, json=document, headers=headers)
    if response.status_code == 201:
        return response.json()['_id']
    elastic_failed_titles.append(course_title)
    return "-1000"

def match_courses_with_description(courses: []) -> None:
    descriptions = get_descriptions()
    for course in courses:
        current_course_description = random.choice(descriptions)
        descriptions.remove(current_course_description)
        
        elastic_id = put_description_to_elastic(current_course_description, course[1])
        
        if(elastic_id == "-1000"):
            descriptions.append(current_course_description)
            continue
        elastic_inds.append(elastic_id)
        with pg_connection.cursor() as cursor:
            q = f'''UPDATE courses SET description_id = '{elastic_id}' WHERE id = {course[0]}; COMMIT;'''
            cursor.execute(q)
        

def generate() -> None:
    match_courses_with_description(get_courses())