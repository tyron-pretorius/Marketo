#https://developers.marketo.com/rest-api/assets/programs/#by_name
#this is a simple function that makes a call to the get program by name endpoint to get the information
#for the program name being queried

import requests

def getProgramByName (base_url, token, name):
    url = base_url + "/rest/asset/v1/program/byName.json?name=" + name

    payload = {}
    headers = {
      'Authorization': 'Bearer ' + token
    }

    response = requests.request("GET", url, headers=headers, data = payload)

    return (response.text)
