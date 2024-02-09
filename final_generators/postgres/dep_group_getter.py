import psycopg2

pg_connection = psycopg2.connect(
    host = 'localhost',
    user = 'fokypoky',
    password = 'toor',
    database = 'university',
    port = 10210
)

def get_specialities() -> []:
    with pg_connection.cursor() as cursor:
        result = []

        query = '''SELECT title, code, study_duration FROM specialties'''
        cursor.execute(query)

        for rd in cursor:
            result.append([rd[0], rd[1], rd[2]])
        
        return result


def get_departments() -> []:
    with pg_connection.cursor() as cursor:
        result = []

        query = '''SELECT d.title, institute_id, s.code FROM departments d join specialties s on d.main_speciality_id = s.id'''
        cursor.execute(query)

        for rd in cursor:
            result.append([rd[0], rd[1], rd[2]])
        
        return result

def get_groups() -> []:
    with pg_connection.cursor() as cursor:
        result = []

        query = '''SELECT number, d.title, s.code FROM groups g join departments d on g.department_id = d.id join specialties s on g.speciality_id = s.id'''
        cursor.execute(query)

        for rd in cursor:
            result.append([rd[0], rd[1], rd[2]])
        
        return result

def write_file(file_path: str, data: []) -> None:
    with open(file_path, 'a', encoding='utf-8') as file:
        text = ''
        for row in data:
            row_str = ''
            for row_data in row:
                row_str += f'{row_data};'
            text += row_str[:-1] + '\n'
        
        file.write(text[:-1])

def read_file(file_path: str) -> []:
    result = []
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            line_values = line.replace('\n', '').split(';')
            line_values_arr = []
            for line_value in line_values:
                line_values_arr.append(line_value)
            result.append(line_values_arr)
    
    return result

pg_connection_2 = psycopg2.connect(
    host = 'mouse.db.elephantsql.com',
    user = 'znejlncd',
    password = 'VYnZHsgxX-74AB5IUmqOWkKDpbWp81Tc',
    database = 'znejlncd',
    port = 5432
)

pg_connection_2.autocommit = True

def insert_specialities(specialities: []) -> None:
    with pg_connection_2.cursor() as cursor:
        query = 'insert into specialities(title, code, study_duration) values '
        for speciality in specialities:
            query += f"""('{speciality[0]}', '{speciality[1]}', {speciality[2]}),"""
        
        cursor.execute(query[:-1])

def insert_departments(departments: list) -> None:
    departments_copy = departments.copy()
    
    # departments processing
    for department in departments_copy:
        with pg_connection_2.cursor() as cursor:
            query = f"select id from specialities where code = '{department[2]}'"
            cursor.execute(query)
            department[2] = cursor.fetchone()[0]
    
    # insert into db
    with pg_connection_2.cursor() as cursor:
        query = 'insert into departments(title, institute_id, main_speciality_id) values'
        for department in departments_copy:
            query += f"""('{department[0]}', {department[1]}, {department[2]}),"""
        cursor.execute(query[:-1])

def insert_groups(groups: list) -> None:
    groups_copy = groups.copy()

    # groups processing
    for group in groups_copy:
        with pg_connection_2.cursor() as cursor:
            query = f"""select id from departments where title = '{group[1]}'"""
            cursor.execute(query)
            group[1] = cursor.fetchone()[0]
        with pg_connection_2.cursor() as cursor:
            query = f"""select id from specialities where code = '{group[2]}'"""
            cursor.execute(query)
            group[2] = cursor.fetchone()[0]
    
    # insert into db
    with pg_connection_2.cursor() as cursor:
        query = 'insert into groups(number, department_id, speciality_id) values'
        for group in groups_copy:
            query += f"""('{group[0]}', {group[1]}, {group[2]}),"""
        cursor.execute(query[:-1])


insert_specialities(read_file('specialities.txt'))
insert_departments(read_file('departments.txt'))
insert_groups(read_file('groups.txt'))