import psycopg2
import pg_database_options as pgoptions
import g4f
import requests

url = 'http://localhost:9200/courses/_doc'
headers = {'Content-Type': 'application/json'}

pg_connection = psycopg2.connect(
        host = pgoptions.host,
        user = pgoptions.user,
        password = pgoptions.password,
        database = pgoptions.db_name,
        port = pgoptions.port
)

failed_lectures = []

def get_course_description(lecture: str, lecture_type: str) -> str:
    try:
        response = g4f.ChatCompletion.create(
        model=g4f.models.gpt_35_turbo,
        messages=[{"role": "user", "content": f"Придумай текст к {lecture_type} на тему '{lecture}'. В ответе укажи только развернутый текст, больше ничего не нужно. ответ дай на русском языке"}],
        )
        print(f'generate successfull: {lecture}')
        return response
    except:
        print(f'failed to generate: {lecture}')
        failed_lectures.append([lecture, response])


def get_all_lectures() -> []:
    lectures = []
    with pg_connection.cursor() as cursor:
        q = '''select l.annotation, lt.type from lectures l join lecture_types lt on l.type_id = lt.id;'''
        cursor.execute(q)
        for raw_data in cursor:
            lectures.append([raw_data[0], raw_data[1]])
    return lectures

print('am a woka woka')
w = input('press any key to exit...')