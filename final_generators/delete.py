import elastic.delete as elad
import mongo_gen.delete as mongen
import postgres.postgres_cleaner as pd
import redis_gen.delete as rd
import neo4j_gen.delete as nd

pd.delete()
print('pg: deleted')

rd.delete()
print('redis: deleted')

nd.delete()
print('neo4j: deleted')

elad.delete()
print('elstic search: deleted')

mongen.delete()
print('mongo: deleted')