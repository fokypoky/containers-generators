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

url = 'http://localhost:10103/lecture_materials/_doc'
headers = {'Content-Type': 'application/json'}

errors = []

def get_lecture_ids() -> []:
    lecture_ids = []
    with pg_connection.cursor() as cursor:
        q = '''SELECT id from lectures;'''
        cursor.execute(q)
        for raw_data in cursor:
            lecture_ids.append(raw_data[0])
    return lecture_ids

def get_materials() -> []:
    with open('./elastic/lectures_description.txt', 'r', encoding='utf-8') as file:
        return file.read().split('/////')

def put_material_to_elastic(materials: str) -> str:
    document = {
        'content': materials
    }
    response = requests.post(url=url, json=document, headers=headers)
    if response.status_code == 201:
        return response.json()['_id']
    return "-1000"

def match_lectures_with_materials(lecture_ids: []) -> None:
    lecture_materials = get_materials()
    with pg_connection.cursor() as cursor:
        for lecture_id in lecture_ids:
            rnd_material = random.choice(lecture_materials)
            elastic_id = put_material_to_elastic(rnd_material)
            if(elastic_id == '-1000'):
                errors.append(f'error to add to {lecture_id}')
                continue
            q = f"""INSERT INTO lecture_materials(lecture_id, materials_id)
                    VALUES({lecture_id}, '{elastic_id}');
                    COMMIT;
            """
            cursor.execute(q)
            lecture_materials.remove(rnd_material)
        

def generate() -> None:
    match_lectures_with_materials(get_lecture_ids())
