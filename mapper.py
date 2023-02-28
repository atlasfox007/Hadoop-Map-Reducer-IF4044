import json
import sys
from datetime import datetime
from dateutil import parser

def youtube_resource(specific_data):
    try:
        return specific_data["crawler_target"]["specific_resource_type"]
    except:
        return None
    
def twitter_resource(specific_data):
    try:
        return specific_data["crawler_target"]["specific_resource_type"]
    except:
        return None

def facebook_resource(specific_data):
    try:
        return specific_data["crawler_target"]["resource_type"]
    except:
        return None

def instagram_resource(specific_data):
    try:
        return specific_data["object"]["social_media"]
    except:
        return None

for line in sys.stdin:
    try:
        # Load the JSON object from the file
        data = json.loads(line)
        for obj in data:
            created_time = obj.get('created_time') or obj.get("created_at") or obj.get("snippet").get(
                "publishedAt") or obj.get("snippet").get("topLevelComment").get("snippet").get("publishedAt")
            resource = youtube_resource(obj) or twitter_resource(
                obj) or facebook_resource(obj) or instagram_resource(obj)
        # Extract the necessary data from the JSON object and emit key-value pairs
            if created_time.isdigit():
                created_time = datetime.fromtimestamp(int(created_time))
            else:
                created_time = parser.parse(created_time, yearfirst=True)
            if(created_time and resource):
                print(f'{created_time.date()}\t{resource}\t1')
    except:
        pass
