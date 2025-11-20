import requests

class SECDataFetcher:
    def __init__(self, user_agent: str):
        """user_agent format: "YourName your.email@example.com" (required by SEC)"""
        
        self.request_delay = 0.11 # SEC only allows 10 requests per second
        self.session = requests.Session()
        self.session.headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov',
        }
        