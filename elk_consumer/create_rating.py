from elasticsearch import Elasticsearch
import csv

es = Elasticsearch("http://localhost:9201")
rating_index_name = "rating_index"

def create_rating_index():
    index_mapping = {
        'properties': {
        'id': {'type': 'integer'},
        'title': {'type': 'keyword'},
        'content': {'type': 'keyword'},
        'customer_id': {'type': 'long'},
        'rating': {'type': 'integer'}
        }
    }

    es.indices.create(index=rating_index_name, mappings=index_mapping)
    print(f"Index '{rating_index_name}' created.")

def read_rating_to_save():
    count = 1 
    with open('/home/linhnq/graduation_project/rating_new.csv', 'r', newline='\n') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            es.index(index=rating_index_name, document=row, request_timeout=60)
            print(str(row['id']) + " " + str(count))
            # if row['id'] == '263487037':
            #     break
            count += 1

    print("Data indexed successfully.")

es.indices.delete(index=rating_index_name)
create_rating_index()

read_rating_to_save()
es.close()