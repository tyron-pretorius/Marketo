# https://developers.marketo.com/rest-api/assets/programs/#update
# When updating program costs, to append new costs, simply add them to your costs array.
# To perform a destructive update, pass your new costs, along with the parameter costsDestructiveUpdate set to true.
# To clear all costs from a program, do not pass a costs parameter, and just pass costsDestructiveUpdate set to true.

# To allow for these above scenarios where a cost parameter may or may not be provided to the function the
# kwargs magic variable is used to pass keyworded arguments to the function, which can then be accessed
# inside the function and stored in the payload by referencing their respective keys

import requests

def updateProgram (base_url,token, pid, **kwargs):

    authorization = "Bearer " + token
    url = base_url + '/rest/asset/v1/program/'+pid+'.json'

    payload = {name: kwargs[name] for name in kwargs if kwargs[name] is not None}

    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache",
        'Authorization': authorization
        }

    response = requests.request("POST", url, data=payload, headers=headers)

    return (response.text)
