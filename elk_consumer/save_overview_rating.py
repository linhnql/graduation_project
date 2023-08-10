from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import happybase
import struct
import json
import time


def get_value(row_data, key, unpack_type=None):
    if key in row_data:
        value = row_data[key]
        if unpack_type:
            value = struct.unpack(unpack_type, value)[0]
        return value.decode('utf-8') if isinstance(value, bytes) else value
    return None

def parse_overview_data(data):
    overview = {}

    stars_data = {}
    for column, value in data.items():
        if column.startswith(b'stars:'):
            _, star, attr = column.split(b':')
            star = int(star.decode())
            if star not in stars_data:
                stars_data[star] = {}
            if attr == b'count':
                stars_data[star]['count'] = struct.unpack('!i', value)[0]
            elif attr == b'percent':
                stars_data[star]['percent'] = struct.unpack('!f', value)[0]

    overview['stars'] = stars_data

    if b'rating:rating_average' in data:
        overview['rating_average'] = struct.unpack('!f', data[b'rating:rating_average'])[0]

    if b'reviews_count:count' in data:
        overview['reviews_count'] = struct.unpack('!i', data[b'reviews_count:count'])[0]

    if b'review_photo:total' in data and b'review_photo:total_photo' in data:
        overview['review_photo'] = {
            'total': struct.unpack('!i', data[b'review_photo:total'])[0],
            'total_photo': struct.unpack('!i', data[b'review_photo:total_photo'])[0]
        }

    return overview

def save_overview_to_elk(message):
    print(message)
    product_id = struct.unpack('!i', message)[0]
    row_data = overview_table.row(message)
    overview = parse_overview_data(row_data)

    product_table_name = 'product'
    product_table = connection.table(product_table_name.encode('utf-8'))

    columns = [b'basic_info:name']
    product_data = product_table.row(struct.pack('!i', product_id), columns=columns)
    name_product = get_value(product_data, b'basic_info:name')

    stars = overview['stars']
    star_fields = {}

    for star, value in stars.items():
        star_fields[f"stars_{star}_count"] = value['count']
        star_fields[f"stars_{star}_percent"] = value['percent']

    doc = {
        'product_id': product_id,
        'name_product': name_product,
        **star_fields,
        'rating_average': overview['rating_average'],
        'reviews_count': overview['reviews_count'],
        'review_photo': overview['review_photo']
    }
    
    es.index(index=overview_index_name, id=product_id, document=doc, request_timeout=60)
    print(f'save es overview rating product {product_id}')

def save_overview_to_elasticsearch(message):
    data = json.loads(message)
    #print(message)
    product_id = data['product_id']
    es.index(index=overview_index_name, id=product_id, document=data, request_timeout=60)
    print(f'save es overview rating product {product_id}')

overview_topic_name = 'overview_topic'
consumer = KafkaConsumer(overview_topic_name, client_id='overview_consumer',
                         bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'])

connection = happybase.Connection(host='localhost', port=9090)
table_name = 'overview_rating'
overview_table = connection.table(table_name.encode('utf-8'))

es = Elasticsearch("http://localhost:9201")
overview_index_name = "overview_rating_index"

print(consumer)
for message in consumer:
    start = time.time()
    save_overview_to_elasticsearch(message.value)
    #print(message.value)
    print(f'{time.time()-start}')


