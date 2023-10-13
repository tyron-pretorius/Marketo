### Code provided by Tyron Pretorius, contact me using tyron@theworkflowpro.com if you have any questions

import requests
import json
from datetime import datetime, timedelta, timezone
import time
import pandas as pd
from io import StringIO

base_url = 'https://xxx-xxx-xxx.mktorest.com'
client_id = 'xxx'
client_secret = 'xxx'
time_frame = 90 #number of days in the past for which you want to extract data
file_path = '/Users/tyronpretorius/Downloads/best_send_time_raw_20230926.csv'
activityIds = [7,10]

def getToken ():
    response = requests.get(base_url+'/identity/oauth/token?grant_type=client_credentials&client_id='+client_id+'&client_secret='+client_secret)

    print(response.text)

    temp = json.loads(response.text)
    token = temp['access_token']
    remaining = temp['expires_in']

    return [token, remaining]

def createJob(startAt, endAt, activityTypeIds, token):

    url = base_url + "/bulk/v1/activities/export/create.json"

    payload = json.dumps({
        "format": 'CSV',
        "filter": {
            "createdAt": {
                "startAt": startAt,
                "endAt": endAt
            },
            "activityTypeIds": activityTypeIds
        }
    })

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)

    temp = json.loads(response.text)
    job_id = temp["result"][0]["exportId"]
    return job_id

def enqueueJob(jobId, token):
    url = base_url + "/bulk/v1/activities/export/"+jobId+"/enqueue.json"

    payload = ""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)

def pollJob(jobId, token):

    url = base_url + "/bulk/v1/activities/export/" + jobId + "/status.json"

    payload = ""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)

    temp = json.loads(response.text)
    status = temp["result"][0]["status"]
    return status

def getJobData(jobId, token):
    url = base_url + "/bulk/v1/activities/export/" + jobId + "/file.json"

    payload = ""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    #print(response.text)

    return (response.text)

def splitTimeFrame(lookBackDays, maxDelta):

    # Get the current timestamp in UTC
    current_timestamp = datetime.now(timezone.utc)

    # Calculate the timestamp 90 days in the past
    past_timestamp = current_timestamp - timedelta(days=lookBackDays)
    print(f"Current: {current_timestamp}, -{lookBackDays}: {past_timestamp}")
    # Calculate the maximum difference between start and end times
    max_duration = timedelta(days=maxDelta)

    time_pairs = []

    end_time = current_timestamp
    more = True

    while more:

        start_time = end_time - max_duration
        if start_time < past_timestamp:
            start_time = past_timestamp
            more = False

        time_pairs.append((start_time.isoformat(), end_time.isoformat()))

        end_time = start_time

    for start_time, end_time in time_pairs:
        print(f"Start: {start_time}, End: {end_time}")

    return (time_pairs)

def createMultipleJobs(time_pairs, token):

    job_ids = []
    for start_time, end_time in time_pairs:
        job_ids.append(createJob(start_time, end_time, activityIds, token))

    return job_ids

def enqueueMultipleJobs(job_ids, token):

    for j in job_ids:
        enqueueJob(j, token)

def waitWhilePolling(job_ids, token):

    last_id = job_ids[-1]
    status = pollJob(last_id,token)
    while status != "Completed":
        time.sleep(30)
        status = pollJob(last_id, token)

def getMultipleJobs(job_ids,token):
    headers = [
        'marketoGUID',
        'leadId',
        'activityDate',
        'activityTypeId',
        'campaignId',
        'primaryAttributeValueId',
        'primaryAttributeValue',
        'attributes'
    ]

    df = pd.DataFrame(columns=headers)

    for j in job_ids:
        raw_data = getJobData(j, token)
        new_data = pd.read_csv(StringIO(raw_data))
        df = pd.concat([df, new_data], ignore_index=True)

    return df

if __name__ == '__main__':

    #Get start and end time pairs needed to create each job
    time_pairs = splitTimeFrame(time_frame, 31)

    #Get an access token and make sure it has more than 6o secs of life
    remaining = 0
    while remaining < 60 :
        time.sleep(remaining)  # if the remaining time is less than 60 secs then wait for the token to expire before getting a new one
        temp = getToken()
        token = temp[0]
        remaining = temp[1]

    #create jobs for each time pair
    job_ids = createMultipleJobs(time_pairs, token)

    #queue jobs & wait until job data is ready
    enqueueMultipleJobs(job_ids, token)
    waitWhilePolling(job_ids,token)

    #get the job data for each job and join together
    df = getMultipleJobs(job_ids, token)

    #write the data to a CSV
    df.to_csv(file_path, index=False)
