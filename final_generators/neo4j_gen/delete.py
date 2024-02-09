from neo4j import GraphDatabase
neo4j_driver = GraphDatabase.driver(uri='bolt://localhost:1933',auth=('neo4j','dr22042002'))
def delete() -> None:
    with neo4j_driver.session() as session:
        session.run('MATCH(n:Courses) DETACH DELETE n;')
        session.run('match(n:Lectures) DETACH DELETE n;')
        session.run('match(n:Students) DETACH DELETE n;')
        session.run('match(n:Groups) DETACH DELETE n;')
        session.run('match(n:Departments) DETACH DELETE n;')
        session.run('match(n:Timetable) DETACH DELETE n;')
        session.run('match(n:Specialities) DETACH DELETE n;')