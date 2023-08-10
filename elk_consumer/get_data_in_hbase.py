import happybase
import struct
import re
import pandas as pd
from elasticsearch import Elasticsearch
import csv
from thrift.transport import TTransport
from kafka import KafkaConsumer

product_topic = 'product_id_topic'
rating_topic_name = 'rating_id_topic'
overview_topic_name = 'overview_id_topic'
product_consumer = KafkaConsumer(product_topic, client_id='product_consumer',
                         bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'])
rating_consumer = KafkaConsumer(rating_topic_name, client_id='rating_consumer',
                         bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'])
# consumer = KafkaConsumer(overview_topic_name, client_id='overview_consumer',
#                          bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'])

connection = happybase.Connection(host='localhost', port=9090)
table_name = 'product'
table = connection.table(table_name.encode('utf-8'))
#row_data = table.row(b'\rw&\x13')
for message in product_consumer:
    # print(struct.unpack('!i', message.value)[0])
    row_data = table.row(message.value)
    # xử lý tiếp 

# row['is_best_store'] = row['is_best_store'].lower() == 'true'
# row['is_leaf'] = row['is_leaf'].lower() == 'true'

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

    text = text.replace("Giá sản phẩm trên Tiki đã bao gồm thuế theo luật hiện hành. Bên cạnh đó, tuỳ vào loại sản phẩm, hình thức và địa chỉ giao hàng mà có thể phát sinh thêm chi phí khác như phí vận chuyển, phụ phí hàng cồng kềnh, thuế nhập khẩu (đối với đơn hàng giao từ nước ngoài có giá trị trên 1 triệu đồng).....", "")
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = text.lower()

    return text.strip()

def save_product_to_csv():
    row_start = 263107023


    count = 1
    prod = 1
    filename = 'product_new.csv'

    with open(filename, 'a', newline='\n') as csv_file:
        fieldnames = ['product_id', 'name', 'price', 'original_price', 'all_time_quantity_sold', 'rating_average',
                      'review_count', 'favourite_count', 'day_ago_created', 'description', 'store_id', 'seller_name',
                      'seller_price', 'is_best_store', 'brand_id', 'brand_name', 'category_id', 'category_name',
                      'is_leaf', 'category_parent_id', 'category_parent_name']

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        stop_program = True

        # while stop_program:
        rows = table.scan()
        # rows = table.scan(row_start=struct.pack('!i', row_start), limit=200)
        # if row_start == 263239283:
        #     print("==================")
        #     rows = []
        #     stop_program = False
        for row_key, row_data in rows:
            count += 1
            prod += 1

            product_id= struct.unpack('!i', row_key)[0]
            # if count == 199:
            #     row_start = product_id
            #     count = 0
            #     break;
            
            writer.writerow({
                'product_id': product_id,
                'name': get_value(row_data, b'basic_info:name'),
                'price': get_value(row_data, b'basic_info:price', '!i'),
                'original_price': get_value(row_data, b'basic_info:original_price', '!i'),
                'all_time_quantity_sold': get_value(row_data, b'basic_info:all_time_quantity_sold', '!i'),
                'rating_average': get_value(row_data, b'basic_info:rating_average', '!f'),
                'review_count': get_value(row_data, b'basic_info:review_count', '!i'),
                'favourite_count': get_value(row_data, b'basic_info:favourite_count', '!i'),
                'day_ago_created': get_value(row_data, b'basic_info:day_ago_created', '!i'),
                'description': preprocess_text(get_value(row_data, b'basic_info:description')),
                'store_id': get_value(row_data, b'current_seller:store_id', '!i'),
                'seller_name': get_value(row_data, b'current_seller:name'),
                'seller_price': get_value(row_data, b'current_seller:price', '!i'),
                'is_best_store': get_value(row_data, b'current_seller:is_best_store', '!?'),
                'brand_id': get_value(row_data, b'brand:id', '!i'),
                'brand_name': get_value(row_data, b'brand:name'),
                'category_id': get_value(row_data, b'categories:id', '!i'),
                'category_name': get_value(row_data, b'categories:name'),
                'is_leaf': get_value(row_data, b'categories:is_leaf', '!?'),
                'category_parent_id': get_value(row_data, b'categories:parent_id', '!i'),
                'category_parent_name': get_value(row_data, b'categories:parent_name')
            })
            print(str(product_id) + "  " + str(count) + "  "  + str(prod))

    
            # save_elasticsearch(data)
            # with open('product.avro', 'a+') as avro_file:
            #     fastavro.writer(avro_file, schema, data)
        # else:
        # # This block executes when the inner loop completes without reaching the break statement,
        # # which means there are no more rows to scan
        #     break

    # df = pd.DataFrame(data)

    connection.close()
    # with open('product.avro', 'wb') as avro_file:
    #     fastavro.writer(avro_file, schema, df.to_dict(orient='records'))

    # return df

def save_rating_to_csv():
    connection = happybase.Connection(host='localhost', port=9090)
    table_name = 'rating'
    table = connection.table(table_name.encode('utf-8'))

    row_start = 263107023


    count = 1
    prod = 1
    filename = 'rating_new.csv'

    with open(filename, 'a', newline='\n') as csv_file:
        fieldnames = ['id','title', 'content', 'customer_id', 'rating']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        stop_program = True

        # while stop_program:
        rows = table.scan()
        # rows = table.scan(row_start=struct.pack('!i', row_start), limit=200)
    
        for row_key, row_data in rows:
            count += 1
            # prod += 1

            rating_id= struct.unpack('!i', row_key)[0]
            writer.writerow({
                'id': struct.unpack('!i', row_key)[0],
                'title': get_value(row_data, b'review_info:title'),
                'content': preprocess_text(get_value(row_data, b'review_info:content')),
                'customer_id': get_value(row_data, b'review_info:customer_id', '!q'),
                'rating': get_value(row_data, b'review_info:rating', '!i')
            })
            print(str(rating_id) + "  " + str(count))# + "  "  + str(prod))
    connection.close()

def save_overview_rating_to_csv():
    connection = happybase.Connection(host='localhost', port=9090)
    table_name = 'overview_rating'
    table = connection.table(table_name.encode('utf-8'))

    row_start = 263107023


    count = 1
    prod = 1
    filename = 'overview_rating.csv'

    with open(filename, 'a', newline='\n') as csv_file:
        fieldnames = ['id','title', 'content', 'customer_id', 'rating']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        stop_program = True

        # while stop_program:
        rows = table.scan()
        # rows = table.scan(row_start=struct.pack('!i', row_start), limit=200)
    
        for row_key, row_data in rows:
            count += 1
            # prod += 1

            rating_id= struct.unpack('!i', row_key)[0]
            writer.writerow({
                'id': struct.unpack('!i', row_key)[0],
                'title': get_value(row_data, b'review_info:title'),
                'content': preprocess_text(get_value(row_data, b'review_info:content')),
                'customer_id': get_value(row_data, b'review_info:customer_id', '!q'),
                'rating': get_value(row_data, b'review_info:rating', '!i')
            })
            print(str(rating_id) + "  " + str(count))# + "  "  + str(prod))
    connection.close()


# save_product_to_csv()
# for index, row in df.iterrows():
#     product_id = row['product_id']
#     product_name = row['basic_info']['name']
#     print(str(product_id) + ": " + product_name)
# save_rating_to_csv()
