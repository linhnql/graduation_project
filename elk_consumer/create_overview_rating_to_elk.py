from elasticsearch import Elasticsearch
import csv, struct, json

es = Elasticsearch("http://localhost:9201")
orating_index_name = "overview_rating_index"


def get_value(row_data, key, unpack_type=None):
    if key in row_data:
        value = row_data[key]
        if unpack_type:
            value = struct.unpack(unpack_type, value)[0]
        return value.decode('utf-8') if isinstance(value, bytes) else value
    return None

def create_orating_index():
    index_mapping = {
        'properties': {
            'product_id': {'type': 'integer'},
            'name_product': {'type': 'keyword'},
            'stars_1_count': {'type': 'integer'},
            'stars_1_percent': {'type': 'float'},
            'stars_2_count': {'type': 'integer'},
            'stars_2_percent': {'type': 'float'},
            'stars_3_count': {'type': 'integer'},
            'stars_3_percent': {'type': 'float'},
            'stars_4_count': {'type': 'integer'},
            'stars_4_percent': {'type': 'float'},
            'stars_5_count': {'type': 'integer'},
            'stars_5_percent': {'type': 'float'},
            'rating_average': {'type': 'float'},
            'reviews_count': {'type': 'integer'},
            'review_photo': {
                'type': 'nested',
                'properties': {
                    'total': {'type': 'integer'},
                    'total_photo': {'type': 'integer'}
                }
            }
        }
    }

    es.indices.create(index=orating_index_name, mappings=index_mapping)
    print(f"Index '{orating_index_name}' created.")
    es.close()

import happybase
from elasticsearch import Elasticsearch

def read_data_from_hbase(connection, table_name):
    table = connection.table(table_name.encode('utf-8'))
    data = []
    rows = table.scan()

    for key, value in rows:
        print(key)
        if len(key) == 4:
            product_id = struct.unpack('!i', key)[0]
        else:
            product_id = int(key.decode('utf-8'))
        overview = parse_overview_data(value)

        data.append({'product_id': product_id, 'overview': overview})

    return data

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

def get_product_name_map():
    product_name_map = {}
    count = 1
    with open('/home/linhnq/graduation_project/product_new.csv', 'r', newline='\n') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            print(count)
            count += 1
            product_name_map[row['product_id']] = row['name']
            
    return product_name_map

def save_data_to_elasticsearch(es, index_name, data, product_name_map):
    count = 1
    for item in data:
        product_id = item['product_id']
        overview = item['overview']
        name_product = product_name_map.get(str(product_id), "") 

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
        
        es.index(index=index_name, id=product_id, document=doc, request_timeout=60)
        print(str(product_id) + " " +str(count))
        count += 1
        if product_id == '263487037':
                break

# if es.indices.exists(index=orating_index_name):
#     # Xóa chỉ mục
#     es.indices.delete(index=orating_index_name)
#     print(f"Chỉ mục '{orating_index_name}' đã được xóa thành công.")
# else:
#     print(f"Chỉ mục '{orating_index_name}' không tồn tại.")
#es.indices.delete(index=orating_index_name)
# create_orating_index()

hbase_connection = happybase.Connection('localhost', port=9090)
hbase_table_name = 'overview_rating'

data = read_data_from_hbase(hbase_connection, hbase_table_name)

map_product = get_product_name_map()
save_data_to_elasticsearch(es, orating_index_name, data, map_product)

print(f"Đã lưu dữ liệu từ bảng HBase vào Elasticsearch.")
