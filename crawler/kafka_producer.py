from kafka import KafkaProducer
import asyncio
import requests

producer = KafkaProducer(bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
                         client_id='category_producer')
topic_name = 'category_topic'

sent_categories = []

def fetch_data():
    url = "https://tiki.vn/api/personalish/v1/blocks/categories?block_code=categories_for_you&page_size=500"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70"
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    return data

def send_to_kafka(category):
    if category in sent_categories:
        return
    print(category)
    producer.send(topic_name, str(category).encode('utf-8'))
    sent_categories.append(category)

async def listen_to_api():
    while True:
        data = fetch_data()
        items = data["items"]
        categories = [(item["id"], item["name"]) for item in items]

        for category in categories:
            send_to_kafka(category)

        await asyncio.sleep(60)  # wait 60s before sending the next request

async def run():
    await listen_to_api()

asyncio.run(run())

