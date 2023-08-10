import requests


class APIManager:
    def __init__(self, headers):
        self.headers = headers

    def get_data(self, url):
        response = requests.get(url, headers=self.headers, timeout=60)
        # time.sleep(1)
        if response.status_code == 200:
            return response.json()
        else:
            return None
