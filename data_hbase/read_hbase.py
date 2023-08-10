#  ================================= all category

# import happybase

# connection = happybase.Connection(host='localhost', port=9090)
# table_name = 'category'

# table = connection.table(table_name)
# count = 0

# for key, data in table.scan():
#     count += 1
#     print(f"Row Key: {key.decode('utf-8')}")
#     print("Data:")
#     for column, value in data.items():
#         print(f"{column.decode('utf-8')}: {value.decode('utf-8')}")
#     print("--------------------")
# print(f'number of category {count}')
# connection.close()


#=================================================review overview
# import happybase
# import struct

# def read_overview_from_hbase(connection, product_id):
#     table_name = f'overview_reviews_{product_id}'

#     # Kết nối tới HBase
#     connection.open()

#     # Lấy bảng từ kết nối
#     table = connection.table(table_name)

#     # Đọc dữ liệu
#     row_key = str(product_id).encode('utf-8')
#     data = table.row(row_key)

#     # Xử lý dữ liệu
#     overview = {}

#     # Xử lý dữ liệu của các cột stars
#     stars_data = {}
#     for column, value in data.items():
#         if column.startswith(b'stars:'):
#             _, star, attr = column.split(b':')
#             if star not in stars_data:
#                 stars_data[star] = {}
#             if attr == b'count':
#                 stars_data[star]['count'] = struct.unpack('!i', value)[0]
#             elif attr == b'percent':
#                 stars_data[star]['percent'] = struct.unpack('!f', value)[0]

#     overview['stars'] = stars_data

#     # Xử lý dữ liệu của cột rating
#     if b'rating:rating_average' in data:
#         overview['rating_average'] = struct.unpack('!f', data[b'rating:rating_average'])[0]

#     # Xử lý dữ liệu của cột reviews_count
#     if b'reviews_count:count' in data:
#         overview['reviews_count'] = struct.unpack('!i', data[b'reviews_count:count'])[0]

#     # Xử lý dữ liệu của cột review_photo
#     if b'review_photo:total' in data and b'review_photo:total_photo' in data:
#         overview['review_photo'] = {
#             'total': struct.unpack('!i', data[b'review_photo:total'])[0],
#             'total_photo': struct.unpack('!i', data[b'review_photo:total_photo'])[0]
#         }

#     return overview

# connection = happybase.Connection(host='localhost', port=9090)  # Cập nhật thông tin kết nối HBase
# overview = read_overview_from_hbase(connection, 435868)
# print(overview)

#================================================= review detail
# import happybase
# import struct

# def get_value(row_data, key, unpack_type=None):
#     if key in row_data:
#         value = row_data[key]
#         if unpack_type:
#             value = struct.unpack(unpack_type, value)[0]
#         return value.decode('utf-8') if isinstance(value, bytes) else value
#     return None

# def read_review_from_hbase():
#     connection = happybase.Connection(host='localhost', port=9090)
#     table_name = f'rating'
#     table = connection.table(table_name.encode('utf-8'))

#     # Lấy tất cả các dòng từ bảng
#     rows = table.scan()

#     # Duyệt qua từng dòng và hiển thị thông tin
#     for row_key, row_data in rows:
#         Đọc thông tin đánh giá
#         review_info = {
#             'title': get_value(row_data, b'review_info:title'),
#             'content': get_value(row_data, b'review_info:content'),
#             'status': get_value(row_data, b'review_info:status'),
#             'thank_count': get_value(row_data, b'review_info:thank_count', '!i'),
#             'score': get_value(row_data, b'review_info:score', '!f'),
#             'new_score': get_value(row_data, b'review_info:new_score', '!f'),
#             'customer_id': get_value(row_data, b'review_info:customer_id', '!q'),
#             'comment_count': get_value(row_data, b'review_info:comment_count', '!i'),
#             'rating': get_value(row_data, b'review_info:rating', '!i')
#         }

#         # Đọc thông tin về hình ảnh
#         images = []
#         image_keys = [key for key in row_data.keys() if key.startswith(b'images:')]
#         for image_key in image_keys:
#             image_id_key = image_key + b':id'
#             image_full_path_key = image_key + b':full_path'
#             image_status_key = image_key + b':status'

#             image = {
#                 'id': get_value(row_data, image_id_key, '!i'),
#                 'full_path': get_value(row_data, image_full_path_key),
#                 'status': get_value(row_data, image_status_key)
#             }

#             if all(image.values()):
#                 images.append(image)

#         # Đọc thông tin về người tạo đánh giá
#         created_by = {
#             'id': get_value(row_data, b'created_by:id', '!i'),
#             'name': get_value(row_data, b'created_by:name'),
#             'full_name': get_value(row_data, b'created_by:full_name'),
#             'joined_time': get_value(row_data, b'created_by:joined_time'),
#             'region': get_value(row_data, b'created_by:region'),
#             'avatar_url': get_value(row_data, b'created_by:avatar_url'),
#             'created_time': get_value(row_data, b'created_by:created_time'),
#             'group_id': get_value(row_data, b'created_by:group_id', '!i'),
#             'purchased': get_value(row_data, b'created_by:purchased', '!?'),
#             'purchased_at': get_value(row_data, b'created_by:purchased_at', '!q'),
#             'total_review': get_value(row_data, b'created_by:total_review', '!i'),
#             'total_thank': get_value(row_data, b'created_by:total_thank', '!i')
#         }

#         # Đọc thông tin về thời gian và quá trình đánh giá
#         timeline = {
#             'review_created_date': get_value(row_data, b'timeline:review_created_date'),
#             'delivery_date': get_value(row_data, b'timeline:delivery_date'),
#             'current_date': get_value(row_data, b'timeline:current_date'),
#             'content': get_value(row_data, b'timeline:content'),
#             'explain': get_value(row_data, b'timeline:explain')
#         }

#         Hiển thị thông tin đánh giá
#         review_id = struct.unpack('!i', row_key)[0]
#         print("Review ID:", review_id)
#         print("Review Info:", review_info)
#         print("Images:", images)
#         print("Created By:", created_by)
#         print("Timeline:", timeline)
#         print("=================")
#         print(row_key.encode('utf-8'))
#         print(get_value(row_data, b'review_info:content'))

#     connection.close()

# read_review_from_hbase()

#============================================================ product
# import happybase
# import struct

# def get_value(row_data, key, unpack_type=None):
#     if key in row_data:
#         value = row_data[key]
#         if unpack_type:
#             value = struct.unpack(unpack_type, value)[0]
#         return value.decode('utf-8') if isinstance(value, bytes) else value
#     return None

# def read_product_from_hbase(connection):
#     table_name = 'product'
#     table = connection.table(table_name.encode('utf-8'))

#     # Scan the entire table
#     rows = table.scan()

#     # List to store the extracted data
#     data = []

#     # Iterate over each row and retrieve information
#     for row_key, row_data in rows:
#         # Read basic information
#         basic_info = {
#             'name': get_value(row_data, b'basic_info:name'),
#             'price': get_value(row_data, b'basic_info:price', '!i'),
#             'original_price': get_value(row_data, b'basic_info:original_price', '!i'),
#             'all_time_quantity_sold': get_value(row_data, b'basic_info:all_time_quantity_sold', '!i'),
#             'rating_average': get_value(row_data, b'basic_info:rating_average', '!f'),
#             'review_count': get_value(row_data, b'basic_info:review_count', '!i'),
#             'favourite_count': get_value(row_data, b'basic_info:favourite_count', '!i'),
#             'day_ago_created': get_value(row_data, b'basic_info:day_ago_created', '!i'),
#             'description': get_value(row_data, b'basic_info:description')
#         }

#         # Read reference information
#         reference = {
#             'short_url': get_value(row_data, b'reference:short_url'),
#             'thumbnail_url': get_value(row_data, b'reference:thumbnail_url')
#         }

#         # Read current seller information
#         current_seller = {
#             'id': get_value(row_data, b'current_seller:id', '!i'),
#             'sku': get_value(row_data, b'current_seller:sku'),
#             'name': get_value(row_data, b'current_seller:name'),
#             'link': get_value(row_data, b'current_seller:link'),
#             'logo': get_value(row_data, b'current_seller:logo'),
#             'price': get_value(row_data, b'current_seller:price', '!i'),
#             'product_id': get_value(row_data, b'current_seller:product_id'),
#             'store_id': get_value(row_data, b'current_seller:store_id', '!i'),
#             'is_best_store': get_value(row_data, b'current_seller:is_best_store', '!?'),
#             'is_offline_installment_supported': get_value(row_data, b'current_seller:is_offline_installment_supported', '!?')
#         }

#         # Read brand information
#         brand = {
#             'id': get_value(row_data, b'brand:id', '!i'),
#             'name': get_value(row_data, b'brand:name'),
#             'slug': get_value(row_data, b'brand:slug')
#         }

#         # Read specifications
#         specifications = {}
#         spec_prefix = b'specifications:'
#         for column, value in row_data.items():
#             if column.startswith(spec_prefix):
#                 _, spec_name, attr_code = column.split(b':')
#                 if spec_name not in specifications:
#                     specifications[spec_name] = {}
#                 specifications[spec_name][attr_code] = get_value(row_data, column)

#         # Read categories information
#         categories = {
#             'id': get_value(row_data, b'categories:id', '!i'),
#             'name': get_value(row_data, b'categories:name'),
#             'is_leaf': get_value(row_data, b'categories:is_leaf', '!?'),
#             'parent_id': get_value(row_data, b'categories:parent_id', '!i'),
#             'parent_name': get_value(row_data, b'categories:parent_name')
#         }

#         # Add the extracted data to the list
#         data.append({
#             'product_id': struct.unpack('!i', row_key)[0],
#             'basic_info': basic_info,
#             'reference': reference,
#             'current_seller': current_seller,
#             'brand': brand,
#             'specifications': specifications,
#             'categories': categories
#         })

#     # Convert the list of dictionaries to a DataFrame
#     df = pd.DataFrame(data)

#     connection.close()

#     return df


import happybase
import struct
import re

def get_value(row_data, key, unpack_type=None):
    if key in row_data:
        value = row_data[key]
        if unpack_type:
            value = struct.unpack(unpack_type, value)[0]
        return value.decode('utf-8') if isinstance(value, bytes) else value
    return None

def preprocess_text(text):
    if text is None:
        return ''
    
    text = text.replace("Giá sản phẩm trên Tiki đã bao gồm thuế theo luật hiện hành. Bên cạnh đó, tuỳ vào loại sản phẩm, hình thức và địa chỉ giao hàng mà có thể phát sinh thêm chi phí khác như phí vận chuyển, phụ phí hàng cồng kềnh, thuế nhập khẩu (đối với đơn hàng giao từ nước ngoài có giá trị trên 1 triệu đồng).....", "")
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = text.lower()

    return text.strip()

def read_product_data(key, row_data):
    product_id = struct.unpack('!i', key)[0]
    name = get_value(row_data, b'basic_info:name')
    rating_average = get_value(row_data, b'basic_info:rating_average', '!f')
    description = preprocess_text(get_value(row_data, b'basic_info:description'))
    seller = get_value(row_data, b'current_seller:name')
    category = get_value(row_data, b'categories:parent_name')

    return product_id, name, category, rating_average, description, seller

connection = happybase.Connection(host='localhost', port=9090)
table_name = 'product'
table = connection.table(table_name.encode('utf-8'))
rows = table.scan()
# Giải mã các giá trị từ dữ liệu đọc được
import csv

# Đường dẫn tới file CSV muốn ghi
csv_file = 'product_full.csv'

# Mở file CSV trong chế độ ghi
with open(csv_file, 'w', newline='') as file:
    writer = csv.writer(file)

    # Ghi tiêu đề cột
    writer.writerow(['id', 'name', 'category','rating_average', 'description', 'seller'])

    # Lặp qua các dòng dữ liệu và ghi vào file CSV
    for key, data in rows:
        product_id, name, category, rating_average, description, seller = read_product_data(key, data)
        writer.writerow([product_id, name, category, rating_average, description, seller])

print("Dữ liệu đã được ghi vào file CSV.")


#import happybase
#import struct

# Kết nối tới HBase
#connection = happybase.Connection('localhost', port=9090)
# Chọn bảng cần truy vấn
#table = connection.table('category')

# Lấy thông tin dòng có row key là '3'
#row = table.row(bytes(str(871), 'utf-8'))
#print(connection)

# In ra các cột và giá trị tương ứng
#for column, value in row.items():
#    print(f'{column.decode()}: {value.decode()}')

# Đóng kết nối
#connection.close()
