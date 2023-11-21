from neo4j import GraphDatabase
neo4j_driver = GraphDatabase.driver(uri='bolt://localhost:1933',auth=('neo4j','dr22042002'))
def delete() -> None:
    with neo4j_driver.session() as session:
        session.run('MATCH(n) DETACH DELETE n;')