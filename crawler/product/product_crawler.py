from thrift.transport.TTransport import TTransportException
from bs4 import BeautifulSoup
import happybase
import logging
import json
import time
import os

from logging.handlers import RotatingFileHandler
from hbase.hbase_manager import HBaseManager
from api_manager.api_manager import APIManager
from rating.rating_crawler import ReviewProcessor
from data.data_producer import DataProducer

log_handler = RotatingFileHandler(
    f'{os.path.abspath(os.getcwd())}/logs/product_crawler.log',
    maxBytes=104857600, backupCount=10)
logging.basicConfig(
    format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
    datefmt='%H:%M:%S',
    level=logging.DEBUG,
    handlers=[log_handler])

class ProductCrawler:
    def __init__(self, headers, hbase_host, hbase_port, category, data_producer):
        self.headers = headers
        self.hbase_host = hbase_host
        self.hbase_port = hbase_port
        # self.data_producer = data_producer
        self.category = category
        self.list_product_api = 'https://tiki.vn/api/personalish/v1/blocks/listings?limit=100&category='
        self.product_api = 'https://tiki.vn/api/v2/products/'
        self.api_manager = APIManager(self.headers)
        self.hbase_manager = HBaseManager(self.hbase_host, self.hbase_port)
        self.data_producer = DataProducer(data_producer)
        self.connection = happybase.Connection(
            host=self.hbase_host, port=self.hbase_port, timeout=600000)  # , protocol='compact')
        self.logger = logging.getLogger('product_scraper')

    def process_product_data(self, product):
        fields = ['id', 'name', 'short_url', 'price', 'original_price', 'rating_average', 'review_count',
                  'favourite_count', 'thumbnail_url', 'day_ago_created', 'description', 'current_seller',
                  'brand', 'specifications', 'all_time_quantity_sold', 'categories']

        message = {field: product.get(field, '') for field in fields}
        message['rating_average'] = float(message['rating_average'])
        message['categories']['parent_id'] = self.category[0]
        message['categories']['parent_name'] = self.category[1]
        
        message['time'] = time.strftime("%Y-%m-%d", time.localtime())
        product_id = message['id']
        product_time = message['time']
        
        message['key'] = f'{product_time}_{product_id}'

        description_html = product.get('description', '')
        description_text = (BeautifulSoup(description_html, 'html.parser')
                            .get_text()
                            .replace("\"", " ")
                            .replace("\\n", " ")
                            .replace("\\x", " ")
                            .replace("\\r", " ").strip())
        message['description'] = description_text

        if not message['brand']:
            message['brand'] = {
                'id': 0,
                'name': '',
                'slug': ''
            }

        specifications = message.get('specifications', [])
        for spec in specifications:
            for attr in spec['attributes']:
                value = attr['value']
                soup = BeautifulSoup(value, 'html.parser')
                clean_value = soup.get_text().replace("\\n", "")
                attr['value'] = clean_value

        if not message['all_time_quantity_sold']:
            message['all_time_quantity_sold'] = 0

        return message

    def save_product(self, product_id, start):
        url = self.product_api + str(product_id)
        product = self.api_manager.get_data(url)
        product = self.process_product_data(product)
        
        #self.hbase_manager.save_product_to_hbase(self.connection, product)
        self.hbase_manager.save_product_day_hbase(self.connection, product)
        print(f'{time.time()-start}')
        self.logger.info(f"{self.category[0]} - Product {product_id} successfully saved to HBase, DONE!!!")
        self.data_producer.send_product_to_kafka(product)
        self.logger.info(f"{self.category[0]} - Product {product_id} sended to Kafka Topic, DONE!!!")

    
    def save_rating(self, product_id, last_product_id, last_page_crawl, page, start):
        review_crawler = ReviewProcessor(self.headers, self.hbase_host, self.hbase_port,
                                        product_id, last_product_id, last_page_crawl, page, self.data_producer, start)
        self.logger.info("Processing rating..., please open rating_crawler logger to see")
        review_crawler.run()
    

    def run(self):
        try:
            url = self.list_product_api + str(self.category[0])
            page_start = 0
            last_product_id = 1
            last_category = 1
            last_page_crawl = 0
            max_retry = 5
            if self.category[0] != last_category:
                page_start = 0
            for page in range(page_start, 21):
                url_with_page = url + f'&page={page}'
                self.logger.info("Processing in " + str(self.category[0]) + " - Page " + str((page)))
                print(str(self.category) + " ==================================> " + str((page)))
                response = self.api_manager.get_data(url_with_page)
                #print(url_with_page)

                if response:
                    data = response['data']
                    found_last_product = False
                    count = 1
                    for product in data:
                        start = time.time()
                        retry_count = 0
                        product_id = product['id']
                        if page == page_start:
                            if not found_last_product:
                                if product_id != last_product_id:
                                    continue
                                else:
                                    found_last_product = True
                        while retry_count < max_retry:
                            try:
                                print(product_id)
                                self.save_product(product_id, start)
                                print(f'product {product_id}') 
                                self.logger.info("Processing Page: " + str(page) + " - Ordinal number of Product: " + str(count))
                                end = time.time()
                                print(f'product hbase,kafka {end-start}')
                                count += 1
                                print(f'rating of product {product_id}')
                                self.save_rating(product_id, last_product_id, last_page_crawl+1, page, start)
                                end = time.time()
                                print(f'rating hbase, kafka{end-start}')
                                break

                            except json.JSONDecodeError as e:
                                self.logger.exception(f"Error parsing JSON: {str(e)}")
                                retry_count += 1
                                time.sleep(3)
                                if retry_count == max_retry:
                                    self.logger.error(f"Reached maximum retry count for product ID {product_id}")
                                    end = time.time()
                                    print(f'{product_id}: {end-start}')
                                    continue
                            #     # continue
                            # except TTransportException as e:
                            #     print("Failed to process product:", e)
                            #     # continue
                            #     retry_count += 1
                            #     if retry_count == max_retry:
                            #         print(
                            #             f"Reached maximum retry count for product ID {product_id}")
                            #         continue


        except Exception as e:
            self.logger.error("Failed to retrieve events from API: ", e)
