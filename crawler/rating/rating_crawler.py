from logging.handlers import RotatingFileHandler
import happybase
import logging
import time
import os

from api_manager.api_manager import APIManager
from hbase.hbase_manager import HBaseManager
from data.data_producer import DataProducer

log_handler = RotatingFileHandler(
    f'{os.path.abspath(os.getcwd())}/logs/rating_crawler.log',
    maxBytes=104857600, backupCount=10)
logging.basicConfig(
    format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
    datefmt='%H:%M:%S',
    level=logging.DEBUG,
    handlers=[log_handler])


class ReviewProcessor:
    def __init__(self, headers, hbase_host, hbase_port, product_id, last_product_id, last_page_crawl, page, data_producer, start):
        self.page = page
        self.last_page_crawl = last_page_crawl
        self.product_id = product_id
        self.last_product_id = last_product_id
        self.review_api = 'https://tiki.vn/api/v2/reviews?limit=20&product_id='
        self.api_manager = APIManager(headers)
        self.hbase_manager = HBaseManager(hbase_host, hbase_port)
        self.data_producer = data_producer
        self.connection = happybase.Connection(
            host=hbase_host, port=hbase_port)
        self.logger = logging.getLogger('rating_scraper')
        self.start = start

    def create_overview(self, review):
        fields = ['stars', 'rating_average', 'reviews_count', 'review_photo']
        review_overview = {field: review.get(field, '') for field in fields}
        return review_overview

    def process_rating(self, review_full):
        fields = ['id', 'title', 'content', 'status', 'thank_count',
                  'score', 'new_score', 'customer_id', 'comment_count', 'rating']
        review = {field: review_full.get(field, '') for field in fields}
        review['score'] = float(review['rating'])
        review['images'] = review_full.get('images', [])
        review['timeline'] = review_full.get('timeline', {})
        created_by = review_full.get('created_by')
        if created_by is None:
            created_by = {}

        contribute_info = created_by.get('contribute_info', {})
        purchased_at = created_by.get('purchased_at', 0)
        purchased_at_value = 0 if purchased_at is None else purchased_at
        review['created_by'] = {
            'id': created_by.get('id', 0),
            'name': created_by.get('name', ''),
            'full_name': created_by.get('full_name', ''),
            'joined_time': contribute_info.get('summary', {}).get('joined_time', ''),
            'region': created_by.get('region', ''),
            'avatar_url': created_by.get('avatar_url', ''),
            'created_time': created_by.get('created_time', ''),
            'group_id': created_by.get('group_id', 0),
            'purchased': created_by.get('purchased', ''),
            'purchased_at': purchased_at_value,
            'total_review': contribute_info.get('summary', {}).get('total_review', 0),
            'total_thank': contribute_info.get('summary', {}).get('total_thank', 0)
        }

        return review

    def run(self):
        url = self.review_api + str(self.product_id)
        response = self.api_manager.get_data(url)

        self.logger.info(
            f'Processing overview rating of product: {self.product_id}')
        review_overview = self.create_overview(response)
        self.hbase_manager.save_overview_to_hbase(
            self.connection, review_overview, self.product_id)
        self.logger.info(
            f'Overview rating successfully saved to HBase, DONE!!!')
        self.data_producer.send_overview_to_kafka(
            self.connection, review_overview, self.product_id)
        print(f'overview: {time.time()-self.start}')
        self.logger.info(
            f'Overview rating successfully sended to Kafka Topic, DONE!!!')

        last_page = response.get('paging', {}).get('last_page', '') + 1
        self.logger.info(
            f'Process to save rating, product {self.product_id} - total {last_page}')
        if self.product_id == self.last_product_id:
            start_page = self.last_page_crawl
        else:
            start_page = 1
        for page in range(start_page, last_page):
            self.logger.info(f'Processing rating in page: {page}')
            url_with_page = url + f'&page={page}'
            review_full = self.api_manager.get_data(url_with_page)
            list_review = review_full.get('data', '')
            #count = 1
            for review in list_review:
                processed_rating = self.process_rating(review)
                if processed_rating['created_by']['id'] == None:
                    continue
                # print(processed_rating)
                self.hbase_manager.save_rating_to_hbase(
                    self.connection, processed_rating)
                self.data_producer.send_rating_to_kafka(processed_rating)
                #count += 1
                #print(f'rating {count} {time.time()-self.start}')
            self.logger.info(f'Saved rating in {self.product_id}: page {page} of {self.page}')
            self.logger.info(f'Sended rating in {self.product_id}: page {page} of {self.page}')

        self.logger.info(
            f'Rating {self.product_id} successfully saved, DONE!!!')

#         url = self.review_api + str(self.product_id) + '&page=207'
#         review_full = self.api_manager.get_data(url)
#         list_review = review_full.get('data', '')
#         for review in list_review:
#             processed_rating = self.process_rating(review)
#             if processed_rating['created_by']['id'] == None: continue
#             self.hbase_manager.save_rating_to_hbase(self.connection, processed_rating, self.product_id)
#         print(f'save rating_{self.product_id}')


# headers = {
#             'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70'}
# hbase_host='localhost'
# hbase_port=9090
# review_crawler = ReviewProcessor(headers, hbase_host, hbase_port, 168115611)
# review_crawler.run()
# review_crawler = ReviewProcessor(headers, hbase_host, hbase_port, 189643105)
# review_crawler.run()
