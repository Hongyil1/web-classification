import requests
import time

st = time.time()
web = requests.get("https://fortiguard.com/webfilter?q=www.brightcloud.com")
# print(web.text)
print(time.time() - st)