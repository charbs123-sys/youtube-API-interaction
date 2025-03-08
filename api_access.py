import requests
import json
import pandas as pd
import os
from dotenv import load_dotenv

""" #marketstack api key
#e1bdcfeed463f61ee6ec654bc450a6a5

url = "https://zillow56.p.rapidapi.com/walk_transit_bike_score"

querystring = {"zpid":"20485700"}

headers = {
	"x-rapidapi-key": "25aae01e03msh0853d226299a03fp1613f9jsne84225b1d822",
	"x-rapidapi-host": "zillow56.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())

as_json = response.json()
dumping = json.dumps(as_json, indent = 2)

resp = json.loads(dumping)
df = pd.json_normalize(resp['data'])
print(df) """

load_dotenv()

def retrieving_channel_id(channel_user, API_KEY):
	url = 'https://www.googleapis.com/youtube/v3/channels?part=id&forUsername='+channel_user+'&key='+API_KEY
	request = json.dumps(requests.get(url).json(), indent=2)
	request = json.loads(request)
	channel_id = request["items"][0]["id"]
	return channel_id


#Youtube API key
API_key = os.getenv("YOUTUBE_API_ROOT_KEY")
channel_user = 'ibm'
channel_id = retrieving_channel_id(channel_user, API_key)
next_page_token = None
""" videos = []


params = {
	'key': API_key,
	'channelId': channel_id,
	'part': 'snippet,id',
	'order': 'date',
	'maxResults': 60,
	'pageToken': next_page_token  # Set the page token for subsequent requests
}

response = requests.get('https://www.googleapis.com/youtube/v3/search', params=params)
data = response.json()

# Collect videos from the current page
videos.extend(data.get('items', []))

# Check if there's a next page token; if not, break loop
next_page_token = data.get('nextPageToken')
print(next_page_token)


print(channel_id)
 """


#1 - extract videoId
def create_df(response, ystats):
	#performs checks (whether we have reached the end of the page count and existance of keys in JSON df), then 
	#populated dataframe ystats
	if 'items' in response:
		for video_id in response['items']:
			if 'id' in video_id and 'videoId' in video_id['id']:
				vid_id = video_id['id']['videoId']
			else:
				continue
			#title_id = video_id['snippet']
			url = 'https://www.googleapis.com/youtube/v3/videos?id='+vid_id+'&part=snippet,statistics&key='+API_key
			response = requests.get(url).json()
			print(response['items'][0]['statistics'])
			if 'viewCount' in response['items'][0]['statistics']:
				view_count = response['items'][0]['statistics']['viewCount']
			else:
				view_count = 0
			if 'likeCount' in response['items'][0]['statistics']:
				like_count = response['items'][0]['statistics']['likeCount']
			else:
				like_count = 0
			if 'favoriteCount' in response['items'][0]['statistics']:
				fav_count = response['items'][0]['statistics']['favoriteCount']
			else:
				fav_count = 0
			if 'commentCount' in response['items'][0]['statistics']:
				com_count = response['items'][0]['statistics']['commentCount']
			else:
				com_count = 0
			if 'title' in response['items'][0]['snippet']:
				title = response['items'][0]['snippet']['title']
			else:
				title = 'NULL'
			new_row = pd.DataFrame([[vid_id, view_count, like_count, fav_count, com_count, title]], 
								columns=['vid_id','view_count','like_count','fav_count','com_count','title'])
			ystats = pd.concat([ystats,new_row], ignore_index = True)
	return ystats



#trying to get data from more videos	-----	

ystats = pd.DataFrame(columns=['vid_id','view_count','like_count','fav_count','com_count', 'title'])
url = 'https://www.googleapis.com/youtube/v3/search?key='+API_key+"&channelId="+channel_id+"&part=snippet,id&order=date&maxResults=50"
counter = 0
while counter < 5:
	counter += 1
	response = requests.get(url).json()
	#print(requests.get(url).url)
	response = json.loads(json.dumps(response, indent = 2))
	ystats = create_df(response, ystats)
	print(ystats)
	if 'nextPageToken' in response:
		url = 'https://www.googleapis.com/youtube/v3/search?key='+API_key+"&channelId="+channel_id+"&part=snippet,id&order=date&maxResults=1&pageToken="+response['nextPageToken']
	else:
		break



print(ystats)
#we succesfully extracted data to be placed in a df, now want to store this in a cloud db like aws
ystats.to_csv("connection.csv")
#print(response)
