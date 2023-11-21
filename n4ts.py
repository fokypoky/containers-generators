from neo4j import GraphDatabase
import random
import datetime
neo4j_driver = GraphDatabase.driver(uri='bolt://localhost:1933',auth=('neo4j','dr22042002'))


start = datetime.datetime.now()
print(f'{start.hour}:{start.minute}:{start.second}')
for i in range(0, 150):
    l = []
    for i in range(0,1000):
        l.append(str(random.randrange(-1000,1000)))

    q = 'CREATE'
    for i in l:
        q += '(:Test{name: "' + i + '"}),'
    q = q[:-1] + ';'

    neo4j_driver.execute_query(q)

end = datetime.datetime.now()
print(f'{end.hour}:{end.minute}:{end.second}')