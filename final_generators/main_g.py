import neo4j_gen.neo4j_generator as n4gen
import postgres.postgres_generator as pgen
import elastic.courses_description as ecourses
import elastic.lectures_description as electures
import mongo_gen.mongo_generator as mongen
import redis_gen.redis_g as rgen
import datetime

start_time = datetime.datetime.today()

# TODO: добавить генерацию в mongo

pgen.generate_specialities()
print('pg: specialities generated')

pgen.generate_departments()
print('pg: departments generated')

pgen.generate_groups()
print('pg: groups generated')

pgen.generate_students()
print('pg: students generated')

rgen.insert()
print('redis: students inserted')

pgen.generate_courses()
print('pg: courses generated')

ecourses.generate()
print('elastic: courses generated')

pgen.generate_lectures()
print('pg: lectures generated')

electures.generate()
print('elastic: lectures generated')

n4gen.put_group_and_students_to_neo4j()
print('neo4j: groups and students inserted')

n4gen.put_departments_and_specialities_to_neo4j()
print('neo4j: departments and specialities inserted')

n4gen.put_lectures_to_neo4j()
print('neo4j: lectures inserted')

n4gen.put_courses_to_neo4j()
print('neo4j: courses inserted')

pgen.generate_timetable()
print('pg: timetable generated')

pgen.generate_visits()
print('pg: visits generated')

n4gen.put_timetable_to_neo4j()
print('neo4j: timetable inserted')

print('in mongo:')
mongen.generate()
print('mongo: university, institutes and departments generated')


end_time = datetime.datetime.today()
print(f'started at {start_time.hour}:{start_time.minute}:{start_time.second}')
print(f'finished at {end_time.hour}:{end_time.minute}:{end_time.second}')