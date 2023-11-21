import elastic.delete as eld
import postgres.postgres_cleaner as pd
import redis_gen.delete as rd
import neo4j_gen.delete as nd

pd.delete()
print('pg: deleted')

rd.delete()
print('redis: deleted')

nd.delete()
print('neo4j: deleted')

eld.delete()
print('elstic search: delete')