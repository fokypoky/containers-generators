import requests
import json

headers = {'Content-Type': 'application/json'}

def delete() -> None:
    document = {
        "query": {
            "match_all": {}
        }
    }
    requests.post(url='http://localhost:9200/courses/_doc/_delete_by_query', headers=headers, json=document)
    requests.post(url='http://localhost:9200/lecture_materials/_doc/_delete_by_query', headers=headers, json=document)