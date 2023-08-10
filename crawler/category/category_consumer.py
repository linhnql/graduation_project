from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from kafka import KafkaConsumer
import happybase
import time
# transport = TFramedTransport(TSocket.TSocket('localhost', 16010))
# protocol = TCompactProtocol.TCompactProtocol(transport)
# client = Hbase.Client(protocol)

# connection = happybase.Connection(host='localhost', port=9090, transport='framed',  protocol='compact')
connection = happybase.Connection(host='localhost', port=9090)
table_name = 'category'
# connection.open()

if table_name.encode() not in connection.tables():
    connection.create_table(
        table_name,
        {'data': dict()}
    )
print(connection)

table = connection.table(table_name)
consumer = KafkaConsumer('category_topic', 
                         bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
                         client_id='category_consumer')

for message in consumer:
    start=time.time()
    id, name = eval(message.value.decode('utf-8'))
    print(f'Process to save {id}')

    # if table.row(bytes(str(id), 'utf-8')):
    #     print(f"{id} already exists in HBase, skipping...")
    #     continue

    table.put(bytes(str(id), 'utf-8'), {'data:name': name})
    print(f"Save {id}, {name} to HBase success {time.time()-start}")

consumer.close()
connection.close()

