import struct

product_topic = 'product_id_topic'
rating_topic_name = 'rating_id_topic'
overview_topic_name = 'overview_id_topic'

class HBaseManager:
    def __init__(self, hbase_host, hbase_port):
        self.hbase_host = hbase_host
        self.hbase_port = hbase_port

    def create_table(self, connection, table_name, column_families):
        #print("save cate - product")
        if table_name.encode('utf-8') not in connection.tables():
            connection.create_table(table_name, column_families)

    def save_data_to_table(self, connection, table_name, row_key, data):
        table = connection.table(table_name.encode('utf-8'))
        with table.batch(batch_size=1000) as batch:
            for column, value in data.items():
                batch.put(row_key, {column: value})

    def check_exist_to_update(self, batch, table, row_key, put_dict):
        existing_data = table.row(row_key)
        #print(table)

        if existing_data:
            existing_data.update(put_dict)  # overwrite data
            batch.put(row_key, existing_data)
        else:
            for column, value in put_dict.items():
                batch.put(row_key, {column: value})

    def save_overview_to_hbase(self, connection, overview, product_id):
        table_name = f'overview_rating'
        column_families = {
            'stars': {},
            'rating': {},
            'reviews_count': {},
            'review_photo': {}
        }
        self.create_table(connection, table_name, column_families)

        table = connection.table(table_name.encode('utf-8'))
        with table.batch(batch_size=1000) as batch:
            row_key = struct.pack('!i', product_id)

            put_dict = {}
            for star, data in overview['stars'].items():
                put_dict.update({
                    f'stars:{star}:count': struct.pack('!i', data['count']),
                    f'stars:{star}:percent': struct.pack('!f', data['percent'])
                })

            put_dict.update({
                b'rating:rating_average': struct.pack('!f', overview['rating_average']),
                b'reviews_count:count': struct.pack('!i', overview['reviews_count']),
                b'review_photo:total': struct.pack('!i', overview['review_photo']['total']),
                b'review_photo:total_photo': struct.pack('!i', overview['review_photo']['total_photo'])
            })

            self.check_exist_to_update(batch, table, row_key, put_dict)


    def save_rating_to_hbase(self, connection, review):
        table_name = f'rating'

        column_families = {
            'review_info': {},
            'images': {},
            'created_by': {},
            'timeline': {}
        }

        self.create_table(connection, table_name, column_families)
        table = connection.table(table_name.encode('utf-8'))

        with table.batch(batch_size=1000) as batch:
            row_key = struct.pack('!i', review['id'])

            put_dict = {}

            put_dict.update({
                b'review_info:title': review['title'].encode('utf-8'),
                b'review_info:content': review['content'].encode('utf-8'),
                b'review_info:status': review['status'].encode('utf-8'),
                b'review_info:thank_count': struct.pack('!i', review['thank_count']),
                b'review_info:score': struct.pack('!f', review['score']),
                b'review_info:new_score': struct.pack('!f', review['new_score']),
                b'review_info:customer_id': struct.pack('!q', review['customer_id']),
                b'review_info:comment_count': struct.pack('!i', review['comment_count']),
                b'review_info:rating': struct.pack('!i', review['rating'])
            })

            for i, image in enumerate(review['images']):
                image_key = f'images:{i}'.encode('utf-8')
                put_dict.update({
                    image_key + b':id': struct.pack('!i', image['id']),
                    image_key + b':full_path': image['full_path'].encode('utf-8'),
                    image_key + b':status': image['status'].encode('utf-8')
                })

            created_time = review['created_by'].get('created_time')
            created_time_value = created_time.encode(
                'utf-8') if created_time is not None else b''
            # print(review)
            put_dict.update({
                b'created_by:id': struct.pack('!i', int(review['created_by'].get('id', 0))),
                b'created_by:name': review['created_by']['name'].encode('utf-8'),
                b'created_by:full_name': review['created_by']['full_name'].encode('utf-8'),
                b'created_by:joined_time': review['created_by']['joined_time'].encode('utf-8'),
                b'created_by:region': review['created_by']['region'].encode('utf-8') if review['created_by']['region'] else b'',
                b'created_by:avatar_url': review['created_by']['avatar_url'].encode('utf-8'),
                b'created_by:created_time': created_time_value,
                b'created_by:group_id': struct.pack('!i', review['created_by']['group_id']),
                b'created_by:purchased': struct.pack('!?', review['created_by']['purchased']),
                b'created_by:purchased_at': struct.pack('!q', review['created_by']['purchased_at']),
                b'created_by:total_review': struct.pack('!i', review['created_by']['total_review']),
                b'created_by:total_thank': struct.pack('!i', review['created_by']['total_thank'])
            })

            put_dict.update({
                b'timeline:review_created_date': review['timeline'].get('review_created_date', '').encode('utf-8'),
                b'timeline:delivery_date': review['timeline'].get('delivery_date', '').encode('utf-8'),
                b'timeline:current_date': review['timeline'].get('current_date', '').encode('utf-8'),
                b'timeline:content': review['timeline'].get('content', '').encode('utf-8'),
                b'timeline:explain': review['timeline'].get('explain', '').encode('utf-8')
            })

            self.check_exist_to_update(batch, table, row_key, put_dict)

    def save_product_to_hbase(self, connection, product):
        table_name = 'product'

        column_families = {
            'basic_info': {},
            'reference': {},
            'current_seller': {},
            'brand': {},
            'specifications': {},
            'categories': {}
        }
        self.create_table(connection, table_name, column_families)
        table = connection.table(table_name.encode('utf-8'))

        with table.batch(batch_size=1000) as batch:
            row_key = struct.pack('!i', product['id'])

            put_dict = {}

            put_dict.update({
                b'basic_info:name': product['name'].encode('utf-8'),
                b'basic_info:price': struct.pack('!i', product['price']),
                b'basic_info:original_price': struct.pack('!i', product['original_price']),
                b'basic_info:all_time_quantity_sold': struct.pack('!i', product['all_time_quantity_sold']),
                b'basic_info:rating_average': struct.pack('!f', product['rating_average']),
                b'basic_info:review_count': struct.pack('!i', product['review_count']),
                b'basic_info:favourite_count': struct.pack('!i', product['favourite_count']),
                b'basic_info:day_ago_created': struct.pack('!i', product['day_ago_created']),
                b'basic_info:description': product['description'].encode('utf-8'),
            })

            put_dict.update({
                b'reference:short_url': product['short_url'].encode('utf-8'),
                b'reference:thumbnail_url': product['thumbnail_url'].encode('utf-8'),
            })

            put_dict.update({
                b'current_seller:id': struct.pack('!i', product['current_seller']['id']),
                b'current_seller:sku': product['current_seller']['sku'].encode('utf-8'),
                b'current_seller:name': product['current_seller']['name'].encode('utf-8'),
                b'current_seller:link': product['current_seller']['link'].encode('utf-8'),
                b'current_seller:logo': product['current_seller']['logo'].encode('utf-8'),
                b'current_seller:price': struct.pack('!i', product['current_seller']['price']),
                b'current_seller:product_id': product['current_seller']['product_id'].encode('utf-8'),
                b'current_seller:store_id': struct.pack('!i', product['current_seller']['store_id']),
                b'current_seller:is_best_store': struct.pack('!?', product['current_seller']['is_best_store']),
                b'current_seller:is_offline_installment_supported': struct.pack('!?', product['current_seller']['is_offline_installment_supported']),
            })

            put_dict.update({
                b'brand:id': struct.pack('!i', product['brand']['id']),
                b'brand:name': product['brand']['name'].encode('utf-8'),
                b'brand:slug': product['brand']['slug'].encode('utf-8'),
            })

            for spec in product['specifications']:
                spec_name = spec['name']
                for attr in spec['attributes']:
                    attr_code = attr['code']
                    attr_value = attr['value']
                    put_dict.update({
                        f'specifications:{spec_name}:{attr_code}': attr_value,
                    })

            put_dict.update({
                b'categories:id': struct.pack('!i', product['categories']['id']),
                b'categories:name': product['categories']['name'].encode('utf-8'),
                b'categories:is_leaf': struct.pack('!?', product['categories']['is_leaf']),
                b'categories:parent_id': struct.pack('!i', product['categories']['parent_id']),
                b'categories:parent_name': product['categories']['parent_name'].encode('utf-8'),
            })

            self.check_exist_to_update(batch, table, row_key, put_dict)
            # self.id_producer.send(product_topic, row_key)
            
            
    def save_product_day_hbase(self, connection, product):
        table_name = 'product_table'
        
        column_families = {
            'basic_info': {},
            'reference': {},
            'current_seller': {},
            'brand': {},
            'specifications': {},
            'categories': {}
        }
        self.create_table(connection, table_name, column_families)
        table = connection.table(table_name.encode('utf-8'))
        

        with table.batch(batch_size=1000) as batch:
            # product_id = product['id']
            # product_time = product['time']
            
            row_key = product['key'].encode('utf-8')
            
            put_dict = {}
            
            put_dict.update({
                b'basic_info:name': product['name'].encode('utf-8'),
                b'basic_info:id': struct.pack('!i', product['id']),
                b'basic_info:price': struct.pack('!i', product['price']),
                b'basic_info:time': product['time'].encode('utf-8'),
                b'basic_info:original_price': struct.pack('!i', product['original_price']),
                b'basic_info:all_time_quantity_sold': struct.pack('!i', product['all_time_quantity_sold']),
                b'basic_info:rating_average': struct.pack('!f', product['rating_average']),
                b'basic_info:review_count': struct.pack('!i', product['review_count']),
                b'basic_info:favourite_count': struct.pack('!i', product['favourite_count']),
                b'basic_info:day_ago_created': struct.pack('!i', product['day_ago_created']),
                b'basic_info:description': product['description'].encode('utf-8'),
            })

            put_dict.update({
                b'reference:short_url': product['short_url'].encode('utf-8'),
                b'reference:thumbnail_url': product['thumbnail_url'].encode('utf-8'),
            })

            put_dict.update({
                b'current_seller:id': struct.pack('!i', product['current_seller']['id']),
                b'current_seller:sku': product['current_seller']['sku'].encode('utf-8'),
                b'current_seller:name': product['current_seller']['name'].encode('utf-8'),
                b'current_seller:link': product['current_seller']['link'].encode('utf-8'),
                b'current_seller:logo': product['current_seller']['logo'].encode('utf-8'),
                b'current_seller:price': struct.pack('!i', product['current_seller']['price']),
                b'current_seller:product_id': product['current_seller']['product_id'].encode('utf-8'),
                b'current_seller:store_id': struct.pack('!i', product['current_seller']['store_id']),
                b'current_seller:is_best_store': struct.pack('!?', product['current_seller']['is_best_store']),
                b'current_seller:is_offline_installment_supported': struct.pack('!?', product['current_seller']['is_offline_installment_supported']),
            })

            put_dict.update({
                b'brand:id': struct.pack('!i', product['brand']['id']),
                b'brand:name': product['brand']['name'].encode('utf-8'),
                b'brand:slug': product['brand']['slug'].encode('utf-8'),
            })

            for spec in product['specifications']:
                spec_name = spec['name']
                for attr in spec['attributes']:
                    attr_code = attr['code']
                    attr_value = attr['value']
                    put_dict.update({
                        f'specifications:{spec_name}:{attr_code}': attr_value,
                    })

            put_dict.update({
                b'categories:id': struct.pack('!i', product['categories']['id']),
                b'categories:name': product['categories']['name'].encode('utf-8'),
                b'categories:is_leaf': struct.pack('!?', product['categories']['is_leaf']),
                b'categories:parent_id': struct.pack('!i', product['categories']['parent_id']),
                b'categories:parent_name': product['categories']['parent_name'].encode('utf-8'),
            })

            self.check_exist_to_update(batch, table, row_key, put_dict)
