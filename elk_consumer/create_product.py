from elasticsearch import Elasticsearch
import csv

es = Elasticsearch("http://localhost:9201")
product_index_name = "product_idx"
product_index_name_ = "product_index"

def create_product_index():
    index_mapping = {
        'properties': {
            'product_id': {'type': 'integer'},
            'name': {'type': 'keyword'},
            'price': {'type': 'integer'},
            'original_price': {'type': 'integer'},
            'all_time_quantity_sold': {'type': 'integer'},
            'rating_average': {'type': 'float'},
            'review_count': {'type': 'integer'},
            'favourite_count': {'type': 'integer'},
            'day_ago_created': {'type': 'integer'},
            'description': {'type': 'text'},
            'store_id': {'type': 'integer'},
            'store_name': {'type': 'keyword'},
            'store_price': {'type': 'integer'},
            'is_best_store': {'type': 'boolean'},
            'brand_id': {'type': 'integer'},
            'brand_name': {'type': 'keyword'},
            'category_id': {'type': 'integer'},
            'category_name': {'type': 'keyword'},
            'is_leaf': {'type': 'boolean'},
            'category_parent_id': {'type': 'integer'},
            'category_parent_name': {'type': 'keyword'}
        }
    }

    es.indices.create(index=product_index_name, mappings=index_mapping)
    print(f"Index '{product_index_name}' created.")

def create_day_product_index():
    index_mapping = {
        'properties': {
            'key': {'type': 'keyword'},
            'id': {'type': 'integer'},
            'name': {'type': 'keyword'},
            'price': {'type': 'integer'},
            'time': {'type': 'date'},
            'original_price': {'type': 'integer'},
            'all_time_quantity_sold': {'type': 'integer'},
            'rating_average': {'type': 'float'},
            'review_count': {'type': 'integer'},
            'favourite_count': {'type': 'integer'},
            'day_ago_created': {'type': 'integer'},
            'description': {'type': 'text'},
            'store_id': {'type': 'integer'},
            'store_name': {'type': 'keyword'},
            'store_price': {'type': 'integer'},
            'is_best_store': {'type': 'boolean'},
            'brand_id': {'type': 'integer'},
            'brand_name': {'type': 'keyword'},
            'category_id': {'type': 'integer'},
            'category_name': {'type': 'keyword'},
            'is_leaf': {'type': 'boolean'},
            'category_parent_id': {'type': 'integer'},
            'category_parent_name': {'type': 'keyword'}
        }
    }

    es.indices.create(index=product_index_name_, mappings=index_mapping)
    print(f"Index '{product_index_name_}' created.")


def read_product_to_save():
    count = 1 
    with open('/home/linhnq/graduation_project/product_new.csv', 'r', newline='\n') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            row['is_best_store'] = row['is_best_store'].lower() == 'true'
            row['is_leaf'] = row['is_leaf'].lower() == 'true'
            es.index(index=product_index_name, document=row, request_timeout=60)
            print(str(row['product_id']) + " " + str(count))
            if row['product_id'] == '263487037':
                break
            count += 1

    print("Data indexed successfully.")

#es.indices.delete(index=product_index_name)
create_product_index()

#es.indices.delete(index=product_index_name_)
create_day_product_index()

# read_product_to_save()

es.close()

