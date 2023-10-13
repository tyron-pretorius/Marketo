import requests
import json

#use the Marketo REST API leads endpoint to update a lead field's with the values contained
#within the input lead_dict
#https://developers.marketo.com/rest-api/lead-database/leads/#create_and_update
def createUpdateLead(base_url, token, lead_dict):

    url = base_url + '/rest/v1/leads.json'

    payload = {
   "action":"updateOnly",
   "lookupField":"id",
   "input": lead_dict
}
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token
    }

    response = requests.request("POST", url, headers=headers, data=json.dumps(payload))

    return (response.text)
