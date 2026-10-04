from datetime import datetime

#return the index and value (in string format) containing the earliest created at date
def createdAt(list, *args):

    time_list= [datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ') for i in list]
    return [time_list.index(min(time_list)), min(time_list).strftime('%Y-%m-%dT%H:%M:%SZ') ]

#return the index and value of the maximum lead score unless the score is highly negative, then return this
#negative value (for bad quality leads)
def leadScore(list, *args):

    new_list = list

    while 'None' in new_list:
        del new_list[new_list.index('None')]

    for i in new_list:
        if i < -10:
            return [list.index(i), i]


    return [list.index(max(new_list)), max(new_list) ]

#return the first index and value where the value is non-null and does not contain "null-like" values
def notNull(list,*args):

    new_list=[x.lower() for x in list]

    crap = ["empty", "unknown", "n/a", "[", "]", 'none']

    for i in new_list:
        good = "true"
        for j in crap:
            if j in i:
                good = False
        if good:
                return [new_list.index(i), list[new_list.index(i)]]

    return [0, list[0]]

#define the prioritized values for certain lead fields in order of descreasing priority from left to right
priority_dict = {
            'website': [".com", ".net", ".org"],
            'country': ["United States", "USA"],
            'leadSource': ["Advertising", "Paid Search", "Organic", "Marketing Generated", "Event",  "Tradeshow",  "Content", "Webinar", "Referral", "Sales Generated","Direct"],
            'leadStatus': ['Disqualified','Customer','Closed Won','SQL','SDR Engaged','SAL','MQL','ReNurture','SSL','Prospects','Prospects Cold','Known','Not a Lead'],
            'Lead_Status__c': ['Disqualified','Closed Won','SQL','SDR Engaged','SAL','MQL','ReNurture','reNurture','SSL','Prospects','Prospects Cold','Known', 'Not a Lead'],
            'Lifecycle_Stage_Person__c': ['Disqualified','Closed Won','SQL','SAL','MQL','reNurture','SSL','Prospects','Prospects Cold', 'Known','Not a Lead'],
        }

#use the priority_dict function to find the highest priority value and corresponding index among the input
#lead values for a certain field. Else if none of the input lead values has a prioritized value return the first
#non-null value and index
def priority(list, line):
    for j in priority_dict[line]:
        for i in list:
            if j in i:
                return [list.index(i), i]

    return(notNull(list))

#return the first TRUE value and its index. True is prioritized because this function is used for the unsubscribed
#account blocked fields where it is important to favor TRUE over FALSE
def boolTest(list, *args):
    for i in list:
        if i is True:
            return [list.index(i), i]

#define the prioritization function that will be called for each field
rules = {
            'createdAt': createdAt,
            'sfdcLeadId': notNull,
            'firstName': notNull,
            'lastName': notNull,
            'company': notNull,
            'title': notNull,
            'website': priority,
            'country': priority,
            #'mcUserId__c': [],
            'Querystring__c': notNull,
            'leadSource': priority,
            #'Lead_Source_Detail__c': [], will pull from the same lead as lead source
            'utm_source__c': notNull,
            #'utm_medium__c': [], will pull from the same lead as utm_source
            #'utm_campaign__c': [], will pull from the same lead as utm_source
            'leadScore': leadScore,
            'leadStatus': priority,
            'Lead_Status__c': priority,
            'Lifecycle_Stage_Person__c': priority,
            'unsubscribed':boolTest,
            'MC_Account_Blocked__c': boolTest,
        }

#create a new list with None converted to string format and then pass this list to the rules dictionary so
#the correct prioritzation function is used on the lead
def ruler (line, line_list):
    formatted_list = []
    for x in line_list:
        if x is None:
            formatted_list.append('None')
        else:
            formatted_list.append(x)
    return rules[line](formatted_list, line)
