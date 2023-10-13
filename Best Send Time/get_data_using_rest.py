import pandas as pd
import requests
import json
import time

base_url = 'https://xxx-xxx-xxx.mktorest.com'
client_id = 'xxx'
client_secret = 'xxx'
activityIds = [7,10]
sinceDate="2023-09-28T00:00:00"
file_path = '/Users/tyronpretorius/Downloads/best_send_time_raw_20230928.csv'

# get an access token
def getToken ():
    response = requests.get(base_url+'/identity/oauth/token?grant_type=client_credentials&client_id='+client_id+'&client_secret='+client_secret)

    print(response.text)

    temp = json.loads(response.text)
    token = temp['access_token']
    remaining = temp['expires_in']

    return [token, remaining]

# get an access token and make sure it has more than 6o secs of life
def checkTokenLife():
    remaining = 0
    while remaining < 60:
        time.sleep(remaining)  # if the remaining time is less than 60 secs then wait for the token to expire before getting a new one
        temp = getToken()
        token = temp[0]
        remaining = temp[1]

    return token

#get the starting token needed to page through activities since the start date
def getStartPage(token, sinceDate):
    url= base_url+'/rest/v1/activities/pagingtoken.json'
    params={'access_token': token,
            'sinceDatetime': sinceDate}
    response=requests.get(url=url,params=params)
    data=response.json()
    print(data)
    return data['nextPageToken']

#get information for the current page
def pagenation(token, nextPageToken):
    url = base_url + '/rest/v1/activities.json'
    params={'access_token': token,
            'nextPageToken': nextPageToken,
            'activityTypeIds': activityIds}
    response=requests.get(url=url,params=params)
    data=response.json()
    print(data)
    return data

if __name__ == '__main__':

    #give more an initial value so we enter the while loop
    more=True

    # get a token with more than 60 secs of life
    token = checkTokenLife()

    # get the starting token needed to page through activities since the start date
    nextPageToken = getStartPage(token, sinceDate)

    #create an empty list to store all the activity information
    activities = []

    #iterate through each page and add the activity info to the list
    while more:

        #get a token with more than 60 secs of life
        token = checkTokenLife()

        #get all the information from this page
        data = pagenation(token, nextPageToken)

        #get the next page token
        nextPageToken = data['nextPageToken']

        #parse the activity information and store it in the list
        activity_info = data['result']
        activities = activities+activity_info

        #if there are more results then this will return True
        more = data['moreResult']

    #Convert the list of json strings to a dataframe
    df=pd.json_normalize(activities)

    #write the data to a CSV
    df.to_csv(file_path, index=False)
