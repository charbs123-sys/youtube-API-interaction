import json 
import pandas as pd
import os
from dotenv import load_dotenv
import requests
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_ROOT_KEY")
channel_user = 'ibm'

url = 'https://www.googleapis.com/youtube/v3/channels?part=id&forUsername='+channel_user+'&key='+API_KEY
request = json.dumps(requests.get(url).json(), indent=2)
request = json.loads(request)
print(request["items"][0]["id"])
#request = requests.get(f'https://www.googleapis.com/youtube/v3/channels', params= {'part':'id', 'forUsername':channel_user, 'key':API_KEY})
