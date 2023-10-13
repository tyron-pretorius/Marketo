import pandas as pd
import numbers
from datetime import datetime
import os
import time

from Priority import ruler
from AppendDict import appendDict
from Marketo_API_Get_Auth import getToken
from Marketo_API_Merge import mergeLead
from Marketo_API_Create_Update_Lead import createUpdateLead

base_url = "https://###-xxx-###.mktorest.com"

#Read in the lead information from a CSV and store in a dictionary
#The email address column must be sorted
raw_list = pd.read_csv('/home/tyron/Downloads/May Merging - Copy From Here.csv')
raw_list = raw_list.where(raw_list.notnull(), None)
raw_list = raw_list.to_dict(orient='records')

#field_dict will store the values for each lead for each field of interest
field_dict = {
    'id': [], 'sfdcLeadId':[], 'email': [], 'createdAt': [], 'firstName': [], 'lastName': [], 'company': [], 'title': [], 'website': [],
    'country': [], 'mcUserId__c': [], 'Querystring__c': [], 'leadSource': [], 'Lead_Source_Detail__c': [],
    'utm_source__c': [], 'utm_medium__c': [], 'utm_campaign__c': [], 'leadScore': [], 'leadStatus': [],
    'Lead_Status__c': [], 'Lifecycle_Stage_Person__c': [], 'unsubscribed': [], 'MC_Account_Blocked__c': [],
}

#final_dict will store the winning values for each field from the input lead values
final_dict = {
    'id': [], 'sfdcLeadId':[], 'email': [], 'createdAt': [], 'firstName': [], 'lastName': [], 'company': [], 'title': [], 'website': [],
    'country': [], 'mcUserId__c': [], 'Querystring__c': [], 'leadSource': [], 'Lead_Source_Detail__c': [],
    'utm_source__c': [], 'utm_medium__c': [], 'utm_campaign__c': [], 'leadScore': [], 'leadStatus': [],
    'Lead_Status__c': [], 'Lifecycle_Stage_Person__c': [], 'unsubscribed': [], 'MC_Account_Blocked__c': []
}

count = 0
update_leads = ['']
i= 0

#create a log file
dateTimeObj = datetime.now()
file_name = dateTimeObj.strftime("%m-%d-%Y_%H:%M:%S")
file_name = "/home/tyron/Downloads/" + file_name + " " + os.path.basename(__file__)
file_name = file_name.replace("py","txt")

remaining = 0
limit = len(raw_list)

while i < limit:

    #if the remaining token life is less than 60 secs then break out of the inner while loop and
    # wait for the token to expire before getting a new one
    time.sleep(remaining)

    count = 0
    start = time.time()
    temp = getToken()
    token = temp[0]
    expires = temp[1]
    print(token, expires)
    remaining = expires - (time.time() - start)

    # Give a 60 sec window so token does not expire mid execution of the loop below
    while i < limit and remaining> 60 :

        field_dict = field_dict.fromkeys(field_dict, [])
        final_dict = final_dict.fromkeys(final_dict, [])

        #while the email address in subsequent rows is the same populate field_dict with the leads' field values
        appendDict(field_dict, raw_list[i])
        j=i+1
        while j < len(raw_list) and (raw_list[i]['email'] == raw_list[j]['email']):
            appendDict(field_dict, raw_list[j])
            j=j+1

        i = j

        #convert id to an int
        for n in range(0, len(field_dict['id'])):
            field_dict['id'][n] = int(field_dict['id'][n])

        #for each field in field_dict compare the input lead values to determine the winning value that will be on the
        #resultant merged lead
        for line in field_dict:
            #if all lead values match then assign the value of the first lead to final dict for this field
            if all(elem==field_dict[line][0] for elem in field_dict[line]) and line != 'sfdcLeadId':
                final_dict[line] =field_dict[line][0]
            else:
                if line in ['email', 'id', 'Lead_Source_Detail__c', 'utm_source__c', 'utm_medium__c' ,'utm_campaign__c', 'mcUserId__c' ]:
                    pass
                #set the sfdcLeadId, id, and mcUserId__c in final_dict to the values from the first lead with a
                # non-null SFDC ID
                elif line == 'sfdcLeadId':
                    [index, value] = ruler(line, field_dict[line])
                    if value is not None:
                        final_dict[line] = value
                        final_dict['id'] = field_dict['id'][index]
                        final_dict['mcUserId__c'] = field_dict['mcUserId__c'][index]
                    #set the createdAt, id, and mcUserId__c in final_dict to the values from the lead that was
                    # created first
                    else:
                        [index, value] = ruler("createdAt", field_dict["createdAt"])
                        final_dict["createdAt"] = value
                        final_dict['id'] = field_dict['id'][index]
                        final_dict['mcUserId__c'] = field_dict['mcUserId__c'][index]
                #set the leadSource, Lead_Source_Detail, and 3xutm fields to the values from the lead with the
                #highest priority leadSource
                elif line == 'leadSource':
                    [index, value]=ruler(line, field_dict[line])
                    final_dict[line] = value
                    final_dict["Lead_Source_Detail__c"] = field_dict["Lead_Source_Detail__c"][index]
                    if field_dict["utm_source__c"][index]: #not empty
                        final_dict["utm_source__c"] = field_dict["utm_source__c"][index]
                        final_dict["utm_medium__c"] = field_dict["utm_medium__c"][index]
                        final_dict["utm_campaign__c"] = field_dict["utm_campaign__c"][index]
                    #if neither lead has leadSource populated then get the 3xutm parameters from the first lead with
                    #non-null utm_source__c
                    else:
                        [index, value] = ruler("utm_source__c", field_dict["utm_source__c"])

                        final_dict["utm_campaign__c"] = field_dict["utm_campaign__c"][index]
                        final_dict["utm_medium__c"] = field_dict["utm_medium__c"][index]
                        final_dict["utm_campaign__c"] = field_dict["utm_campaign__c"][index]
                #pass the field and lead values for this field to the ruler function in the priority script to
                #obtain the winning field value according to the rules specified in the functions inside the
                #priority script
                else:
                    final_dict[line] = ruler(line, field_dict[line])[1]

        #log the the values from each lead for all of the fields of interest i.e. field_dict
        f = open(file_name, "a")
        dateTimeObj = datetime.now()
        f.write(dateTimeObj.strftime("%m-%d-%Y_%H:%M:%S") + "\t" + str(count) + "\t" + str(i) + "\n\n")
        f.write("Winning and losing lead information:\n")
        f.write(str(field_dict) + "\n\n")
        f.write("Winning ID with field and value combinations to be updated after merging:\n")

        print(count)
        print(i)
        print(field_dict)
        print(final_dict)
        print('')

        #store the losing lead id(s)
        loser_ids = field_dict['id']
        loser_ids.remove(final_dict['id'])

        #convert leadScore from float to int to prevent merge failure and store the result in update_leads
        update_leads[0] = final_dict
        if isinstance(update_leads[0]['leadScore'], numbers.Number):
            update_leads[0]['leadScore'] = int(update_leads[0]['leadScore'])

        #log the winning field values that the resultant merged lead with be updated with
        f.write(str(update_leads[0]) + "\n\n")

        #merge the leads and log the response returned
        response = mergeLead(base_url, token, final_dict['id'], loser_ids, True)
        f.write("Merge Response:\n")
        f.write(str(response)+"\n\n")
        print(response)

        #if the merged failed then close the log file
        if '"success":false' in str(response):
            f.write("---------------------------------------------------------------------------------------\n\n")
            f.close()
        #else update the merged lead with the winning field values from update_leads and log the response
        else:
            response = createUpdateLead(base_url, token, update_leads)
            print(response)
            f.write("Update Response:\n")
            f.write(response + "\n\n")
            n = 0
            #Even if the conditional logic above determines the lead id to come from Person A, if Person B is a contact
            #and Person A is a lead in Salesforce then Marketo's merge method will ensure that Person B is the winner.
            #Hence, after a successful merge if the update response contains skipped then cycle through the rest of
            #the "losing" ids to find the actual winner i.e. the id of the one lead that still exists
            while '"status":"skipped"' in str(response) and n < len(loser_ids):
                update_leads[0]['id'] = loser_ids[n]
                print(update_leads[0])
                f.write(str(update_leads[0]) + "\n\n")
                response = createUpdateLead(base_url, token, update_leads)
                print(response)
                f.write("Update Response:\n")
                f.write(response + "\n\n")
                n=n+1

            f.write("---------------------------------------------------------------------------------------\n\n")
            f.close()

        count = count + 1

        #sleep for 0.2secs to ensure that Marketo's REST API limit of 100 calls per 20 secs is not exceeded
        time.sleep(0.2)

        #recalculate the remaining token lifespan before moving to the next merge
        remaining = expires - (time.time() - start)
        #sometimes the time elapsed can be greater than the original lifespan of the token, hence the reason for
        #setting remaining to zero below to prevent the time.sleep method from failing
        if remaining < 0:
            remaining = 0
