import pandas as pd
from datetime import datetime
import os
import time

import json
import re

from Marketo_API_Get_Auth import getToken
from Marketo_API_Get_Program_By_Name import getProgramByName
from Marketo_API_Update_Program import updateProgram

#read the pivot table into a Pandas dataframe
df = pd.read_csv('/home/tyron/Downloads/Paid Marketing_ Marketo Program Cost - Tyron Desired Output (1).csv')

base_url = "https://###-xxx-###.mktorest.com"

#create a timestamped log file
dateTimeObj = datetime.now()
file_name = dateTimeObj.strftime("%m-%d-%Y_%H:%M:%S")
file_name = "/home/tyron/Downloads/" + file_name + " " + os.path.basename(__file__)
file_name = file_name.replace("py","txt")

remaining = 0
i =0

#the iterrows() function is used to iterate over the DataFrame rows as (index, Series) pairs.
#iterates over the DataFrame columns, returning a tuple with the column name and the content as a Series.
#https://www.w3resource.com/pandas/dataframe/dataframe-iterrows.php
iterrows = list(df.iterrows())
wait = 60

while i < len(df.index):

    # if the remaining token life is less than 60 secs then break out of the inner while loop and
    # wait for the token to expire before getting a new one
    time.sleep(remaining)

    start = time.time()

    #get the Marketo API access token and the length of time it is valid for
    temp = getToken()
    token = temp[0]
    expires = temp[1]
    print(token, expires)

    #calculate the time remaining on the access token life
    remaining = expires - (time.time() - start)

    # Give a 60 sec window so token does not expire mid execution of the loop below
    while remaining > wait and i < len(df.index):

        #get the ith row from the dataframe in (index, series) pair format e.g. ('Marketo Program', 'Program Name')('7/1/2019', 'Cost 1')('8/1/2019, 'Cost 2')
        row = iterrows[i][1]

        costs=[]

        #iterate over the (index, series) pairs
        for date, cost in row.iteritems():
            if date == 'Marketo Program':

                #log the program name
                f = open(file_name, "a")
                dateTimeObj = datetime.now()
                f.write(dateTimeObj.strftime("%m-%d-%Y_%H:%M:%S") + "\t" + cost + "\n\n")
                print(cost)

                #query Marketo for the program information by name and store the response
                #https://developers.marketo.com/rest-api/assets/programs/#by_name
                response = getProgramByName(base_url, token, cost)
                f.write('Get Program Response: ' + "\t" + response + "\n\n")
                print(response)

                #if the program is found parse the response to get the program id and the createdAt date
                #with the createdAt date set to the first of the month for comparison with the pivot table dates
                if "No assets found" not in response:
                    result = re.search('result":\[(.*)\]}', response).group(1)
                    dict = json.loads(result)
                    pid = str(dict["id"])
                    created_at = datetime.strptime(dict["createdAt"],'%Y-%m-%dT%H:%M:%SZ+0000').replace(day=1).date()

                else:
                    break
            else:
                date_object = datetime.strptime(date, '%m/%d/%Y').date()

                #if the cost for the pivot table date in this row is not empty and the Marketo program existed before
                #this date or was created in the same month then append the (date, cost) pair to the costs list
                if pd.notnull(cost) and created_at <= date_object:
                    costs.append({"startDate":str(date_object),"cost":int(round(cost))})
                    print(date,cost)

        if len(costs)>0:

            #pass the program id and costs list to the updateProgram function
            #setting costsDestructiveUpdate=True will clear out any costs that are stored in the program for the
            #months that are in the costs list, which is desired since the costs in the (date, cost) pairs are the
            #ad spend for the entirety of the months.
            #if you would like to preserve the existing costs in the program then set costsDestructiveUpdate=False
            #then the incoming costs in the costs list will be appended to the existing costs, and all the costs
            #that now exist for a month will be summed and used to get the cost per lead for that month
            #https://developers.marketo.com/rest-api/assets/programs/#update
            f.write('Costs: ' + "\t" + str(costs) + "\n\n")
            response = updateProgram(base_url, token, pid, costs=str(costs), costsDestructiveUpdate=True)
            f.write('Update Program Response: ' + "\t" + response)
            print(response)
            f.write("\n\n---------------------------------------------------------------------------------------\n\n")
            f.close()
            
        #the program will enter the else statement if there was no program found for the campaign name in the pivot
        #table or the campaign was found but there were no costs values after the program was created
        else:
            if "No assets found" in response :
                f.write('Program Not Found')
            else:
                f.write('Nothing to update')

            f.write("\n\n---------------------------------------------------------------------------------------\n\n")
            f.close()

        #implement a 0.2sec delay so that Marketo's REST API limit of 100 calls per 20 seconds is not exceeded
        time.sleep(0.2)
        i=i+1
        remaining = expires - (time.time() - start)

        # sometimes the time elapsed can be greater than the original lifespan of the token, hence the reason for
        # setting remaining to zero below to prevent the time.sleep method from failing
        if remaining < 0:
            remaining = 0

