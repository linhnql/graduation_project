from product.product_crawler import ProductCrawler

from kafka import KafkaConsumer, KafkaProducer


hbase_host='localhost'
hbase_port=9090
headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70'}

done = [871, 844, 845, 1795, 861, 1084, 1856, 842, 853, 872, 385, 847, 1630, 664, 848, 1754, 9733, 5246, 840, 1948, 53074, 8400, 2657, 1838, 6750,
	9725, 23622, 1794, 5121, 1367, 2136, 885, 843, 2514, 854, 886, 3177, 2225, 1936, 2324, 3320, 23588, 4428, 11878, 45546, 4381, 2553, 7358,
	855, 4423, 2340, 9724, 3865, 2008, 8084, 24076, 8239, 3422, 2249, 23710, 1521, 17170, 28454, 849, 53190, 7671, 5531, 6305, 17162, 1613,
	1984, 49656, 8123, 369,]
cate_done = [871, 844, 845, 1795, 861, 1084, 1856, 842, 853, 872, 385, 847, 1630, 664, 848, 1754, 9733, 5246, 840, 1948, 53074, 8400, 2657, 1838,
            6750, 9725, 23622, 1794, 5121, 1367, 2136, 885, 843, 2514,  854, 886, 3177, 2225, 1936, 2324, 3320, 23588, 4428, 11878, 45546, 4381,
            2553, 7358, 855, 4423, 2340, 9724, 3865, 2008, 8084, 24076, 8239, 3422, 2249, 23710, 1521, 17170, 28454, 849, 53190, 7671, 5531, 6305, 17162, 1613, 1984, 49656]
#done_new = [871, 1795, 844,842,1856,845,2324,861,1613,8400,1936,1984,2553,664,886,1794,840,872,853,17170,49656,28574,8123,385,1583,2136,5121,8275,
#53074,847,843,9733,1918,5267,2971,28454,23622,2552,45546,28452,1583,1634,8305,2153,2602,2106,885,9120,1630,3422,6750,5246,23446,2025,8084,6280,5267]
arr = [871, 844, 842, 845, 1795, 664, 840, 861, 1856, 886, 2602, 872, 9725, 17170, 49656, 853, 6750, 9733, 24062, 665, 385, 593, 8123, 843, 4423, 1754, 23622, 9724, 1984, 885, 5531, 1583, 2328, 23376, 2136, 6522, 849, 3862, 855, 9120, 1794, 8400, 848, 847, 1936, 4293, 5267, 45546, 4239, 1630, 854, 53074, 6548, 5246, 6305, 2110, 28932, 6280, 7358, 23362, 1918]

consumer = KafkaConsumer('category_topic', client_id='category_consumer',
                         bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'])

producer = KafkaProducer(bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
                         client_id='id_producer')
print(consumer)

for message in consumer:
# for message in done:3862,5267,665,11605,28452,1583,1634,8305,2153,2602,2106, cate này:23446, page 11, sp
    # if message in cate_done: continue
    # print(f'{message}')
    # product_crawler = ProductCrawler(headers, hbase_host, hbase_port, message)
    # product_crawler.run()
    # cate_done.append(message)

    category = eval(message.value.decode('utf-8')) # Change Tuple (id, name) - (848, 'Sách Marketing - Bán hàng')
    #if category[0] in arr:  continue
            
    product_crawler = ProductCrawler(headers, hbase_host, hbase_port, category, producer)
    product_crawler.run()
    #done_new.append(category[0])

# consumer.close()
