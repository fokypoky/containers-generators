import psycopg2
import pg_database_options as pgoptions
import g4f
import time

url = 'http://localhost:9200/courses/_doc'
headers = {'Content-Type': 'application/json'}

pg_connection = psycopg2.connect(
        host = pgoptions.host,
        user = pgoptions.user,
        password = pgoptions.password,
        database = pgoptions.db_name,
        port = pgoptions.port
)

failed_courses = []

def get_course_description(course: str) -> str:
    try:
        response = g4f.ChatCompletion.create(
        model=g4f.models.gpt_35_turbo,
        messages=[{"role": "user", "content": f"Придумай описание к курсу '{course}'. В ответе укажи только развернутый текст о чем будет этот курс, больше ничего не нужно. ответ дай на русском языке"}],
        )
        print(f'generate successfull: {course}')
        return response
    except:
        print(f'failed to generate: {course}')
        failed_courses.append([course, response])

def get_lecture_description(lecture: str, lecture_type: str) -> str:
    response = g4f.ChatCompletion.create(
    model=g4f.models.gpt_35_turbo,
    messages=[{"role": "user", "content": f'Придумай текст к {lecture_type} на тему {lecture}. В ответе укажи только текст к {lecture_type} и ничего больше. Ответ дай на русском языке'}],
    )
    return response


def generate_texr(request: str) -> str:
    response = g4f.ChatCompletion.create(
    model=g4f.models.gpt_35_turbo,
    messages=[{"role": "user", "content": request}],)
    return response

def save_annotations():
    annot = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT annotation from lectures;'''
        cursor.execute(q)
        for rd in cursor:
            annot.append(rd[0])
    for a in annot:
        append_file(a)

def get_all_courses() -> []:
    courses = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT title from courses where id between 281 and 321 and id not between 300 and 312;'''
        cursor.execute(q)
        for raw_data in cursor:
            courses.append(raw_data[0])
    return courses

def append_file(text: str) -> None:
    with open('lecture_annotations.txt', 'a', encoding='utf-8') as file:
        file.write(f'{text}\n/////')

def generate_courses_description(courses: []) -> None:
    descriptions = []
    for course in courses:
        append_file(get_course_description(course))

def get_handled_lectures() -> []:
    with open('generated_ids.txt', 'r', encoding='utf-8') as file:
        return file.read().split('\n')

def generate_lecture_desctiptions() -> None:
    lectures = [] # id ,annotation, type
    handled_lectures = get_handled_lectures()
    with pg_connection.cursor() as cursor:
        q = '''select l.id, l.annotation, lt.type from lectures l join lecture_types lt on l.type_id = lt.id order by l.id;'''
        cursor.execute(q)
        for raw_data in cursor:
            if str(raw_data[0]) in handled_lectures:
                continue
            lectures.append([raw_data[0], raw_data[1], raw_data[2]])
    
    counter = len(lectures)
    for lecture in lectures:
        print(f'less: {counter}')
        lecture_materials = get_lecture_description(lecture=lecture[1], lecture_type=lecture[2])
        with open('lectures_description.txt', 'a', encoding="utf-8") as file:
            file.write(f'{lecture_materials}\n/////')
        with open('generated_ids.txt', 'a', encoding='utf-8') as file:
            file.write(f'{lecture[0]}\n')
        counter -= 1

def generate_reqs(lecture_annotation: str, lecture_type: str) -> str:
    response = g4f.ChatCompletion.create(
    model=g4f.models.gpt_35_turbo,
    messages=[{"role": "user", "content": f'Придумай технические требования к {lecture_type} на тему "{lecture_annotation}". Дай короткий ответ(не более 100 символов) на русском языке.'}],)
    return response

def generate_lectures_req() -> None:
    lectures = []
    handled_lectures = get_handled_lectures()
    with pg_connection.cursor() as cursor:
        q = 'SELECT l.id, l.annotation, lt.type from lectures l join lecture_types lt on l.type_id = lt.id order by l.id;'
        cursor.execute(q)
        for rd in cursor:
            if str(rd[0]) not in handled_lectures:
                lectures.append([rd[0], rd[1], rd[2]])
    counter = len(lectures)
    for lecture in lectures:
        print(f'less: {counter}')
        lecture_reqs = generate_reqs(lecture_annotation=lecture[1], lecture_type=lecture[2])
        with open('lecture_req.txt', 'a', encoding="utf-8") as file:
            file.write(f'{lecture_reqs}\n/////')
        with open('generated_ids.txt', 'a', encoding='utf-8') as file:
            file.write(f'{lecture[0]}\n')
        counter -= 1

def get_reqs_from_file() -> []:
    with open('lecture_req.txt', 'r', encoding='utf-8') as file:
        return list(filter(lambda s: s.strip(), file.read().split('/////')))

max_length = 0
for req in get_reqs_from_file():
    if len(req) > max_length:
        max_length = len(req)

print(max_length)