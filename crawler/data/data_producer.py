import struct
import json

product_topic = 'product_topic'
rating_topic = 'rating_topic'
overview_topic = 'overview_topic'

def get_value(row_data, key, unpack_type=None):
    if key in row_data:
        value = row_data[key]
        if unpack_type:
            value = struct.unpack(unpack_type, value)[0]
        return value.decode('utf-8') if isinstance(value, bytes) else value
    return None

class DataProducer:
    def __init__(self, data_producer):
        self.producer = data_producer

    def send_overview_to_kafka(self, connection, overview, product_id):
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

        data = {
            'product_id': product_id,
            'name_product': name_product,
            **star_fields,
            'rating_average': overview['rating_average'],
            'reviews_count': overview['reviews_count'],
            'review_photo': overview['review_photo']
        }
        
        data_bytes = json.dumps(data).encode('utf-8')
        self.producer.send(overview_topic, data_bytes)


    def send_rating_to_kafka(self, review):
        data = {
            'id': review['id'],
            'title': review['title'],
            'content': review['content'],
            'customer_id': review['customer_id'],
            'rating': review['rating']
        }
        
        data_bytes = json.dumps(data).encode('utf-8')
        self.producer.send(rating_topic, data_bytes)

    def send_product_to_kafka(self, product):
        data = {
            'key': product['key'],
            'id': product['id'],
            'name': product['name'],
            'price': product['price'],
            'time': product['time'],
            'original_price': product['original_price'],
            'all_time_quantity_sold': product['all_time_quantity_sold'],
            'rating_average': product['rating_average'],
            'review_count': product['review_count'],
            'favourite_count': product['favourite_count'],
            'day_ago_created':  product['day_ago_created'],
            'description': product['description'],
            'store_id': product['current_seller']['store_id'],
            'is_best_store': product['current_seller']['is_best_store'],
            'seller_name': product['current_seller']['name'],
            'seller_price': product['current_seller']['price'],
            'brand_id': product['brand']['id'],
            'brand_name': product['brand']['name'],
            'category_id': product['categories']['id'],
            'category_name': product['categories']['name'],
            'category_parent_id': product['categories']['parent_id'],
            'category_parent_name': product['categories']['parent_name']
        }
        
        data_bytes = json.dumps(data).encode('utf-8')        
        self.producer.send(product_topic, data_bytes)
