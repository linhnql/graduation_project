# # =====================================  list product cate 871 - product 184466860

# import struct
# import happybase
# from six import iteritems
# connection = happybase.Connection(host='localhost', port=9090)
# table_name = 'product'

# table = connection.table(table_name.encode('utf-8'))

# rows = table.scan(columns=['basic_info'])
# str_delete = "Giá sản phẩm trên Tiki đã bao gồm thuế theo luật hiện hành. Bên cạnh đó, tuỳ vào loại sản phẩm, hình thức và địa chỉ giao hàng mà có thể phát sinh thêm chi phí khác như phí vận chuyển, phụ phí hàng cồng kềnh, thuế nhập khẩu (đối với đơn hàng giao từ nước ngoài có giá trị trên 1 triệu đồng)....."
# for key, data in rows:
#     row_key = struct.unpack('!i', key)[0]
#     print(f"=============================================== product_id: {row_key}")

#     for column in data.keys():
#         cf_name, col_name = column.split(b':')
#         cf_name = cf_name.decode('utf-8')
#         col_name = col_name.decode('utf-8')

#         value = data[column]

#         if col_name == 'name':
#             col_value = value.decode('utf-8')
#         elif col_name == 'description':
#             col_value = value.decode('utf-8').replace(str_delete, "")
#         elif col_name == 'rating_average':
#             col_value = struct.unpack('!f', value)[0] 
#         else:
#             col_value = struct.unpack('!i', value)[0]

#         print(f"Column family: {cf_name}")
#         print(f"Column: {col_name}, Value: {col_value}")
# connection.close()

from pyvi import ViTokenizer
from collections import Counter
import happybase

word_counter = Counter()

connection = happybase.Connection(host='localhost', port=9090)
table_name = 'product'
table = connection.table(table_name.encode('utf-8'))

rows = table.scan(columns=['basic_info:description'])
str_delete = "Giá sản phẩm trên Tiki đã bao gồm thuế theo luật hiện hành. Bên cạnh đó, tuỳ vào loại sản phẩm, hình thức và địa chỉ giao hàng mà có thể phát sinh thêm chi phí khác như phí vận chuyển, phụ phí hàng cồng kềnh, thuế nhập khẩu (đối với đơn hàng giao từ nước ngoài có giá trị trên 1 triệu đồng)....."

count = 1

for key, data in rows:
    try:
        for column, value in data.items():
            cf_name, col_name = column.split(b':')
            cf_name = cf_name.decode('utf-8')
            col_name = col_name.decode('utf-8')

            if col_name == 'description':
                col_value = value.decode('utf-8').replace(str_delete, "")
            else:
                col_value = ""

            tokens = ViTokenizer.tokenize(col_value)
            print(count)
            count += 1
            word_counter.update(tokens)
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        continue

for word, frequency in word_counter.items():
    print(f"Word: {word}, Frequency: {frequency}")

