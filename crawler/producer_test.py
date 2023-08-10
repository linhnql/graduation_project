from kafka import KafkaProducer
import asyncio
import requests

producer = KafkaProducer(bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
                         client_id='category_producer')
topic_name = 'category_topic'

id_cate = 1984
name = 'Chảo các loại'

#(9733, 'Kiến Thức Bách Khoa')
#(1984, 'Chảo các loại')

async def listen_to_api():
    category = (id_cate, name)
    print(category)
    producer.send(topic_name, str(category).encode('utf-8'))
    while True:
        pass

async def run():
    await listen_to_api()

asyncio.run(run())

