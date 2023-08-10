from logging.handlers import RotatingFileHandler
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import happybase
import struct
import json
import time
import re


def get_value(row_data, key, unpack_type=None):
    if key in row_data:
        value = row_data[key]
        if unpack_type:
            value = struct.unpack(unpack_type, value)[0]
        return value.decode('utf-8') if isinstance(value, bytes) else value
    return None


def preprocess_text(text):
    if text is None:
        return ''

    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = text.lower()

    return text.strip()

def save_rating_to_csv(message):
    print(message)
    rating_id = struct.unpack('!i', message)[0]
    columns = [b'review_info:title', b'review_info:content', b'review_info:customer_id', b'review_info:rating']
    row_data = table.row(message, columns=columns)
    
    data = {
        'id': rating_id,
        'title': get_value(row_data, b'review_info:title'),
        'content': preprocess_text(get_value(row_data, b'review_info:content')),
        'customer_id': get_value(row_data, b'review_info:customer_id', '!q'),
        'rating': get_value(row_data, b'review_info:rating', '!i')
    }

    es.index(index=rating_index_name, id=rating_id, document=data, request_timeout=60)
    #print(f'save ex rating {rating_id}')
    
def save_rating_to_elasticsearch(message):
    data = json.loads(message)
    #print(message)
    rating_id = data['id']
    es.index(index=rating_index_name, id=rating_id, document=data, request_timeout=60)
    print(f'save ex rating {rating_id}')

rating_topic_name = 'rating_topic'
rating_consumer = KafkaConsumer(rating_topic_name, client_id='rating_consumer',
                         bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'])

connection = happybase.Connection(host='localhost', port=9090)
table_name = 'rating'
table = connection.table(table_name.encode('utf-8'))

es = Elasticsearch("http://localhost:9201")
rating_index_name = "rating_index"

#row_data = table.row(b'\rw&\x13')
print(rating_consumer)
for message in rating_consumer:
    # print(struct.unpack('!i', message.value)[0])
    start = time.time()
    save_rating_to_elasticsearch(message.value)
    #print(message.value)
    print(f'{time.time()-start}')
