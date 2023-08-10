import faiss
import numpy as np
from pyvi import ViTokenizer, ViUtils
from pyspark.sql import SparkSession
import struct
from pyspark.ml.feature import Word2VecModel
import pandas as pd
import re
import happybase

spark = SparkSession.builder.getOrCreate()


def read_data_from_hbase(connection, table_name):
    table = connection.table(table_name.encode('utf-8'))
    data = []

    for key, value in table.scan():
        product = {}
        product['id'] = struct.unpack('!i', key)[0]
        product['name'] = (preprocess_text(value[b'basic_info:name'].decode('utf-8'))
                           .replace("hàng chính hãng", "")
                           .replace("chính hãng", "")
                           .replace("hàng nhập khẩu", "")
                           .replace("nhập khẩu", "")
                           .replace("chi tiết", "")
                           .replace("phân phối", ""))

        data.append(product)

    return data

def preprocess_text(text):
    processed_text = re.sub(r"[^\w\s]", "", text.lower().strip())
    return processed_text

def tokenize_and_extract_words(text):
    try:
        # words = ViUtils.remove_accents(ViTokenizer.tokenize(text)).decode("utf-8").split(" ")
        words = ViTokenizer.tokenize(text).split(" ")
        return words
    except IndexError as e:
        print("IndexError:", e)
        return []

# product_data = [
#         {'id': 1, 'name': 'tai nghe nhét tai audio technica athckx5is'},
#         {'id': 2, 'name': 'tai nghe nhét tai audio technica atkbhx5is'},
#         {'id': 3, 'name': 'tai nghe chụp tai audio technica athm50x'},
#         {'id': 4, 'name': 'hộp đựng thực phẩm chữ nhật kova colorful life 750ml'},
#         {'id': 5, 'name': 'tai nghe nhét tai sony fontopia mdre9lp'},
#         {'id': 6, 'name': 'tai nghe chụp tai sony mdrzx110ap'},
#         {'id': 7, 'name': 'tai nghe nhét tai sony mdrex15ap'},
#         {'id': 8, 'name': 'hộp cơm điện hâm nóng đa năng magic a03 105l'},
#         {'id': 9, 'name': 'loa bluetooth isound sp12 16w'},
#         {'id': 10, 'name': 'máy massage mini cầm tay beurer mg16'},
#         {'id': 11, 'name': 'tai nghe urbanista san francisco'},
#         {'id': 12, 'name': 'loa di động microlab t6 36w'},
#         {'id': 13, 'name': 'chuột quang không dây genius nx 7000'},
#         {'id': 14, 'name': 'chuột không dây genius nx7015'},
#         {'id': 15, 'name': 'chuột không dây forter v181'},
#         {'id': 16, 'name': 'ngủ ngon hẹn mai nhé'},
#         {'id': 17, 'name': 'tai nghe nhét tai audio technica athcks770is'},
#         {'id': 18, 'name': 'tai nghe chụp tai audio technica athws550is'},
#         {'id': 19, 'name': 'chuột không dây logitech m325'},
#         {'id': 20, 'name': 'tủ tabi duy tân 5 ngăn họa tiết ngẫu nhiên'}
#     ]

def find_similar_products(data, product_name, model, index, spark,top_k=5):
    product_name = preprocess_text(product_name)
    tokens = tokenize_and_extract_words(product_name)

    product_df = spark.createDataFrame([(tokens,)], ["tokens"])
    
    product_df = model.transform(product_df)

    _vector = np.array([product_df.select("features_word2vec").collect()[0][0]], dtype=np.float32)

    faiss.normalize_L2(np.ascontiguousarray(_vector))

    distances, ann = index.search(_vector, k=top_k)
    results = pd.DataFrame({'distances': distances[0], 'ann': ann[0]})
    merge = pd.merge(results, data, left_on='ann', right_index=True)[['id', 'name']]
    #print(merge)
    merge = merge[merge['name'] != product_name]

    return merge


connection = happybase.Connection(host='localhost', port=9090)
table_name = 'product'

model_path = "word2vec_model"
index_path = "vector_index.faiss"
top_k = 20

product_data = read_data_from_hbase(connection, table_name)
data = pd.DataFrame(product_data, columns=['id', 'name'])


word2vec_model = Word2VecModel.load(model_path)
index = faiss.read_index(index_path)
spark = SparkSession.builder.getOrCreate()

while True:
    product_name = input("Enter a product name (or press Enter to quit): ")
    if not product_name:
        break

    similar_products = find_similar_products(data, product_name, word2vec_model, index, spark, top_k)
    print("List Recommendation Product")
    print(similar_products)
