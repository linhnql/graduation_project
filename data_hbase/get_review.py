import csv
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
    text = re.sub(r'\s+', ' ', text)        # loai bo khoang trang thua
    text = re.sub(r'[^\w\s]', '', text)     # loai bo ky tu dac biet
    text = text.lower()
    # text = re.sub(r'(.)\1+', r'\1', text)   # loai ky tu trung 
    text = re.sub(r'([A-Za-z])\1+', r'\1', text)
    text = re.sub(r'\b(good|god|gud|nice|acepted|ok|ok[a-zA-Z]+)\b', r' hài lòng ', text)
    text = re.sub(r'\b(k|ko|kg|kh|khg|khog|khong)\b', r'không', text)
    text = re.sub(r'\b(aps)\b', r'ứng dụng', text)
    text = re.sub(r'\b(sp)\b', r'sản phẩm', text)
    text = re.sub(r'\b(iu)\b', r'yêu', text)
    return text.strip()


def read_review_from_hbase():
    connection = happybase.Connection(host='localhost', port=9090)
    table_name = 'rating'
    table = connection.table(table_name.encode('utf-8'))

    rows = table.scan()

    with open('rating.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(['content', 'label'])

        for row_key, row_data in rows:
            rating = get_value(row_data, b'review_info:rating', '!i')
            content = get_value(row_data, b'review_info:content')


            if content is not None and content.strip() != '':
                content = preprocess_text(content)
                if content == '': continue

                if rating in [1, 2]:
                    label = 'negative'
                elif rating == 3:
                    label = 'neutral'
                elif rating in [4, 5]:
                    label = 'positive'
                else:
                    label = 'Unknown'

                writer.writerow([content, label])

    connection.close()


read_review_from_hbase()
# print(preprocess_text("k nhanhhhhhhh okkkkk kh good 4444445455555"))