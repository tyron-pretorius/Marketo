import requests
import json
from datetime import datetime, timedelta
import pytz
import time
import pandas as pd
from io import StringIO
from urllib.parse import parse_qs
import phpserialize


base_url = 'https://xxx-xxx-xxx.mktorest.com'
client_id = 'xxx'
client_secret = 'xxx'
start_date = "2023-08-01"
end_date = "2023-10-17" #put this one day ahead of the final day you want e.g. if you want to extract all the data from 9-30 then set this to 10-1
file_path = '/Users/tyronpretorius/Downloads/Trust Radius/' #make sure this directory exists before running the code
activityIds = [2,11]

#-------------------------- Extracting Data Functions ------------------------

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

def splitTimeFrame(start_date,end_date, maxDelta):

    cdt_timezone = pytz.timezone('America/Chicago')

    start_cdt = cdt_timezone.localize(datetime.strptime(start_date, "%Y-%m-%d"))
    end_cdt = cdt_timezone.localize(datetime.strptime(end_date, "%Y-%m-%d"))
    print(start_cdt,end_cdt)

    start = start_cdt.astimezone(pytz.UTC)
    end = end_cdt.astimezone(pytz.UTC)
    print(start, end)

    max_duration = timedelta(days=maxDelta)

    time_pairs = []

    end_time = end
    more = True

    while more:

        start_time = end_time - max_duration
        if start_time < start:
            start_time = start
            more = False

        time_pairs.append((start_time.isoformat(), end_time.isoformat()))

        end_time = start_time

    for start_time, end_time in time_pairs:
        print(f"Start: {start_time}, End: {end_time}")

    return (time_pairs)

def createMultipleJobs(time_pairs, token, activityIds):

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

def getActivityOverRange(activityID):
    # Get start and end time pairs needed to create each job
    time_pairs = splitTimeFrame(start_date, end_date, 31)

    # Get an access token and make sure it has more than 6o secs of life
    remaining = 0
    while remaining < 60:
        time.sleep(remaining)  # if the remaining time is less than 60 secs then wait for the token to expire before getting a new one
        temp = getToken()
        token = temp[0]
        remaining = temp[1]

    # create jobs for each time pair
    job_ids = createMultipleJobs(time_pairs, token, [activityID])

    # queue jobs & wait until job data is ready
    enqueueMultipleJobs(job_ids, token)
    waitWhilePolling(job_ids, token)

    # get the job data for each job and join together
    df = getMultipleJobs(job_ids, token)

    return df

#-------------------------- Activity 2 Processing Functions ------------------------
def extract_fields(serialized_str):
    try:
        json_data = json.loads(serialized_str)
        webpage_id = json_data.get('Webpage ID', None)
        user_agent = json_data.get('User Agent', None)
        referrer_url = json_data.get('Referrer URL', None)
        client_ip_address = json_data.get('Client IP Address', None)
        query_parameters = json_data.get('Query Parameters', None)
        form_fields = json_data.get('Form Fields', None)
        return form_fields, webpage_id, user_agent, referrer_url, client_ip_address, query_parameters,
    except Exception as e:
        print(f"Error: {e}")
        return None, None, None, None, None, None

# Function to parse the query string parameters
def parse_query_parameters(query_str):
    try:
        params = parse_qs(query_str)
        # Convert the values from lists to single values
        params = {k: v[0] for k, v in params.items()}
        return params
    except Exception as e:
        print(f"Error: {e}")
        return {}

# Function to deserialize PHP serialized data
def deserialize_php(data):
    try:
        return phpserialize.loads(data.encode(), decode_strings=True)
    except Exception as e:
        print(f"Error in deserializing: {e}")
        return None

def processActivity2 (df):
    # Convert 'activityDate' to datetime format
    df['activityDate'] = pd.to_datetime(df['activityDate'])

    df['activityDate'] = df['activityDate'].dt.tz_convert('America/Chicago')

    # Extract new columns
    df['Form Fields'], df['Webpage ID'], df['User Agent'], df['Referrer URL'], df['Client IP Address'], df['Query Parameters'] = zip(*df['attributes'].apply(extract_fields))

    # Parse the query parameters and add them as new columns
    df_query_params = df['Query Parameters'].apply(parse_query_parameters).apply(pd.Series)
    df = pd.concat([df, df_query_params], axis=1)

    # Deserialize the 'Form Fields' column
    df['Form Fields'] = df['Form Fields'].apply(deserialize_php)

    # Extract month from 'activityDate' and group by it
    grouped = dict(tuple(df.groupby(df['activityDate'].dt.month)))

    # Save each month's dataframe to a separate CSV file
    for month, df in grouped.items():
        file_name = f"{file_path}processed_trust_radius_activity2_month_{month}.csv"
        df.to_csv(file_name, index=False)

#-------------------------- Activity 11 Processing Functions ------------------------

# Check if the 'attributes' column contains valid JSON strings
def is_valid_json(json_string):
    try:
        json.loads(json_string)
        return True
    except:
        return False

def processActivity11 (trust_radius_df):
    # Filter out rows where 'attributes' column does not contain valid JSON
    trust_radius_df = trust_radius_df[trust_radius_df['attributes'].apply(is_valid_json)]

    # Convert the 'attributes' column strings to dictionaries
    trust_radius_df['attributes'] = trust_radius_df['attributes'].apply(json.loads)

    # Expand the 'attributes' column into individual columns
    expanded_attributes_df = trust_radius_df['attributes'].apply(pd.Series)
    expanded_df = pd.concat([trust_radius_df.drop('attributes', axis=1), expanded_attributes_df], axis=1)

    # Convert columns to appropriate data types based on the "september-11.csv" format
    cols_to_convert = {
        'Campaign Run ID': 'int64',
        'Is Mobile Device': 'bool',
        'Step ID': 'int64',
        'Choice Number': 'int64',
        'Is Bot Activity': 'bool'
    }

    for col, dtype in cols_to_convert.items():
        expanded_df[col] = expanded_df[col].astype(dtype)

    # Convert the 'activityDate' column to datetime type and transform from UTC to CDT
    expanded_df['activityDate'] = pd.to_datetime(expanded_df['activityDate']).dt.tz_convert('America/Chicago')

    # Extract month from 'activityDate' and group by it
    grouped = dict(tuple(expanded_df.groupby(expanded_df['activityDate'].dt.month)))

    # Save each month's dataframe to a separate CSV file
    for month, df in grouped.items():
        file_name = f"{file_path}processed_trust_radius_activity11_month_{month}.csv"
        df.to_csv(file_name, index=False)


if __name__ == '__main__':

    dfs = []

    #when running this code for the first time we need to create the jobs and extract the data
    for id in activityIds:
        df = getActivityOverRange(id)
        # write the data to a CSV
        df.to_csv(file_path+"trust_radius_activity"+str(id)+".csv", index=False)
        dfs.append(df)

    #if the code failed for some reason and we already have the data then comment out the for loop above and use these lines of code
    #dfs= [pd.read_csv(file_path+"trust_radius_activity2.csv"), pd.read_csv(file_path+"trust_radius_activity11.csv")]

    processActivity2(dfs[0])
    processActivity11(dfs[1])
