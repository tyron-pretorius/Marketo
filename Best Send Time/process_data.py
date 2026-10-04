import pandas as pd
import re

file_path = '/Users/tyronpretorius/Downloads/'
input_name = 'best_send_time.csv'
opened_id = 10
delivered_id = 7

# Define a function to add "NA" and the missing activity
def process_activity(row):
    if 'Delivered' not in row['activity']:
        row['activityDate'] = 'NA, ' + row['activityDate']
        row['activity'] = 'Delivered, ' + row['activity']
    if 'Opened' not in row['activity']:
        row['activityDate'] = row['activityDate'] + ', NA'
        row['activity'] = row['activity'] + ', Opened'

    # in cases where the opened time is the same as the delivery time reverse the activity order so Delivered comes first
    if re.match(r'^Opened, Delivered', row['activity']):
        row['activity'] = re.sub(r'^Opened, Delivered', "Delivered, Opened", row['activity'])

    activities = row['activity'].split(', ')
    if len(activities) > 2:
        print(row)
        activity_dates = row['activityDate'].split(', ')
        opened_idx = activities.index('Opened')
        delivered_idx = opened_idx - 1  # there will always be a Delivered event before an opened event
        row['activity'] = "Delivered, Opened"
        row['activityDate'] = activity_dates[delivered_idx] + ", " + activity_dates[opened_idx]
        print(row)

    return row

if __name__ == '__main__':

    #Read in CSV
    #See "Raw Data" Google Sheet
    df = pd.read_csv(file_path+input_name)
    df['attributes'] = df['attributes'].astype(str)

    # Filter out bot activity
    filtered_rows = df[(df['activityTypeId'] == opened_id) & (df['attributes'].str.contains('"Is Bot Activity":true'))]
    df = df.drop(filtered_rows.index)

    #Drop excess columns
    df = df.drop(columns=['marketoGUID','campaignId','primaryAttributeValue', 'attributes'], axis=1)

    #Replace 7 with Delivered and 10 with Opened
    df['activityTypeId'] = df['activityTypeId'].replace({delivered_id: 'Delivered', opened_id: 'Opened'})

    #Rename Columns
    df = df.rename(columns={'activityTypeId':'activity', 'primaryAttributeValueId':'emailId'})

    #Sort by activityDate
    df = df.sort_values(by='activityDate')

    # See "After 1st Tidy Up" Google Sheet
    df.to_csv(file_path+'best_send_time_1st_tidy.csv', index=False)

    # Group the data by 'leadId' and 'emailId' and aggregate the data
    grouped_df = df.groupby(['leadId', 'emailId']).agg({
        'activityDate': ', '.join,  # Join activity values with comma
        'activity': ', '.join  # Join activity values with comma
    }).reset_index()

    # See "Group by Lead & Email" Google Sheet
    grouped_df.to_csv(file_path+'best_send_time_group_lead_email.csv', index=False)

    # Add 'Na' for missing activities and timestamps
    grouped_df = grouped_df.apply(process_activity, axis=1)

    # See "Insert NA" Google Sheet
    grouped_df.to_csv(file_path+'best_send_time_insert_na.csv', index=False)

    # Group the data by 'leadId'
    grouped_df_2 = grouped_df.groupby(['leadId']).agg({
        'activityDate': ' || '.join,  # Get the earliest activityDate
    }).reset_index()

    # See "Group by Lead" Google Sheet
    grouped_df_2.to_csv(file_path+'best_send_time_group_lead.csv', index=False)
