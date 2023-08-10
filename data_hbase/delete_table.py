# import happybase


# def delete_hbase_tables(table_names):
#     connection = happybase.Connection('localhost', port=9090)

#     for table_name in table_names:
#         try:
#             connection.delete_table(table_name, disable=True)
#             print(f"Deleted table '{table_name}' successfully.")
#         except Exception as e:
#             print(f"Error deleting table '{table_name}': {e}")

#     connection.close()


# table_list = ["category", "category_1084", "category_1794", "category_1795", "category_1838", "rating_991732", "rating_9943779", "rating_9985547", "rating_99870571"]

# delete_hbase_tables(table_list)
from elasticsearch import Elasticsearch
from random import randint

# Kết nối tới Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Sử dụng API update để cập nhật giá trị của trường favourite_count trong các tài liệu ngẫu nhiên
for _ in range(9737):
    # Chọn một tài liệu ngẫu nhiên bằng API search với kích thước tài liệu 1
    response = es.search(
        index='product_index',
        body={
            "size": 1,
            "query": {
                "function_score": {
                    "random_score": {},
                    "boost_mode": "replace"
                }
            }
        }
    )

    # Lấy ID của tài liệu
    document_id = response['hits']['hits'][0]['_id']

    # Tạo một số nguyên ngẫu nhiên
    new_favourite_count = randint(1, 19584)

    # Sử dụng API update để cập nhật giá trị của trường favourite_count trong tài liệu
    es.update(
        index='product_index',
        id=document_id,
        body={
            "doc": {
                "favourite_count": new_favourite_count
            }
        }
    )
