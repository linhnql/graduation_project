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
    
def save_product_to_elasticsearch(message):
    data = json.loads(message)
    #print(message)
    product_key = data['key']
    es.index(index=product_index_name, id=product_key, document=data, request_timeout=60)
    print(f'save es product {product_key}')

product_topic = 'product_topic'
product_consumer = KafkaConsumer(product_topic, client_id='product_consumer',
                         bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'])

es = Elasticsearch("http://localhost:9201")
product_index_name = "product_index"

print(product_consumer)
for message in product_consumer:
    start = time.time()
    save_product_to_elasticsearch(message.value)
    print(f'{time.time()-start}')