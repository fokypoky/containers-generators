import psycopg2
import pg_database_options as pgoptions
import g4f
import requests
#from elasticsearch import Elasticsearch

pg_connection = psycopg2.connect(
        host = pgoptions.host,
        user = pgoptions.user,
        password = pgoptions.password,
        database = pgoptions.db_name,
        port = pgoptions.port
)

failed = []
url = 'http://26.172.236.37:9200/courses/_doc'
headers = {'Content-Type': 'application/json'}


def get_courses() -> []:
    courses = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT title from courses;'''
        cursor.execute(q)
        for raw_data in cursor:
            courses.append(raw_data[0])
    return courses

def get_course_description(course: str) -> str:
    try:
        response = g4f.ChatCompletion.create(
        model=g4f.models.gpt_35_turbo,
        messages=[{"role": "user", "content": f"Придумай описание к курсу '{course}'. В ответе укажи только развернутый текст о чем будет этот курс, больше ничего не нужно"}],
        )
        print(f'generate successfull: {course}')
        return response
    except:
        print(f'failed to generate: {course}')
        failed.append([course, response])

def insert_data_to_elastic_search( course_name: str, course_data: str):
    document = {
        'title' : course_name,
        'content' : course_data
    }
    try:
        response = requests.post(url= url, json= document, headers= headers)
        print(f'successfull: {course_name}')
    except:
        failed.append([course_name, response])
        print(f'failed: {course_name}')


def generate_courses_info():
    courses = get_courses()
    for course in courses:
        print(course)
        description = get_course_description(course)
        insert_data_to_elastic_search(course_name=course,course_data=description)


generate_courses_info()

# document = {
#     'title': 'testtitle',
#     'content': 'knock kbock knuckle poga'
# }
#

# headers = {'Content-Type': 'application/json'}
#
# request = requests.post(url=url, json=document, headers=headers)
# print(request.status_code)
# print(request.text)