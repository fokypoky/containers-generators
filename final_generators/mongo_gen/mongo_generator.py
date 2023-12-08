import psycopg2
import pymongo

pg_connection = psycopg2.connect(
    host = 'localhost',
    user = 'fokypoky',
    password = 'toor',
    database = 'university',
    port = 10210
)

department_institute = {
    '1': 'Институт кибербезопасности и цифровых технологий',
    '2': 'Институт информационных технологий',
    '3': 'Институт искусственного интеллекта',
    '4': 'Институт перспективных технологий и индустриального программирования',
    '5': 'Институт радиоэлектроники и информатики',
    '6': 'Институт технологий управления',
    '7': 'Институт тонких химических технологий им. М.В. Ломоносова'
}

def get_departments() -> []:
    departments = []
    with pg_connection.cursor() as cursor:
        q = 'SELECT title, institute_id from departments;'
        cursor.execute(q)
        for rd in cursor:
            departments.append([rd[0], str(rd[1])])
    return departments

def get_document() -> {}:
    departments = get_departments() # title, institute id
    document = {
        "university_id": 1,
        "name": "РТУ МИРЭА"
    }

    institutes = []
    department_index = 1
    for key in department_institute.keys():
        institute_doc = {
            'institute_name': department_institute[key],
            'institute_id': key
        }

        current_institute_departments = []
        for department in departments:
            if department[1] != key:
                continue
            
            department_document = {
                'department_name': department[0],
                'department_id': department_index
            }

            current_institute_departments.append(department_document)
            department_index += 1
        
        institute_doc['departments'] = current_institute_departments
        institutes.append(institute_doc)

    document['institutes'] = institutes
    return document

def put_document_to_mongo(document: {}) -> None:
    client = pymongo.MongoClient('localhost', 27017)
    db = client['UniversityDB']
    university_collection = db['university']
    university_collection.insert_one(document)

def generate() -> None:
    put_document_to_mongo(get_document())

generate()