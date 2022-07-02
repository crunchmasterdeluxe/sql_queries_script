#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import dropbox
import mysql.connector
import pandas as pd
import datetime
import numpy as np
import time
from datetime import date
import httplib2
import os
from apiclient import discovery
from google.oauth2 import service_account
import re
import requests
import json


def upload(dbx, dataframe, folder, name, overwrite=True):
    path = '/%s/%s' % (folder, name)
    while '//' in path:
        path = path.replace('//', '/')
    mode = (dropbox.files.WriteMode.overwrite
            if overwrite
            else dropbox.files.WriteMode.add)
    
 
    df = dataframe.to_csv(index=False).strip('\n').split('\n')
    df_string = '\r\n'.join(df)
    data = df_string.encode('utf8')
    try:
        res = dbx.files_upload(
            data, path, mode,
            #client_modified=datetime.datetime.now(),
            client_modified=datetime.datetime.now() + datetime.timedelta(hours = 9),
            mute=True)
    except dropbox.exceptions.ApiError as err:
        print('*** API error', err)
        return None
    print('uploaded as', res.name.encode('utf8'))
    return res

def toLocal(appt_datetime,lat,lng):

    obj = TimezoneFinder()
    tz = obj.timezone_at(lng = float(lng), lat = float(lat))

    local = pytz.timezone(tz)
    naive = datetime.datetime.strptime(f"{appt_datetime}", "%Y-%m-%d %H:%M:%S")
    local_dt = local.localize(naive, is_dst=None) #change for daylight savings
    #utc_dt = local_dt.astimezone(pytz.utc)
    date_time = local_dt.strftime("%Y-%m-%d %H:%M:%S")

    return (date_time)



CANVASS_HOST="XXXXX"
CANVASS_USER="XXXXX"
CANVASS_PASSWORD="XXXXX"
CANVASS_DATABASE="XXXXX"

df = df[1:]
failed_list=[]
appts_held=[]
appts_not_held=[]
for (a,b,c,d,e,f) in zip(df[0],df[3],df[12],df[13],df[14],df[2]):
    if d == 'Yes' or d == 'yes' or d == '1' or d == 1:
        if f in ['Address Not Matched','Address Not Geocoded','Lead creation failed','Address Not Created']:
            appts_held.append(a)
        try:
            db = mysql.connector.connect(
                            host=CANVASS_HOST,
                            user=CANVASS_USER,
                            passwd=CANVASS_PASSWORD,
                            database=CANVASS_DATABASE
            )
            cursor = db.cursor()
            cursor.execute(f"UPDATE appointments SET held = 1, marked_held_date = '{datetime.date.today()}' WHERE id = {b}")
            db.commit()
            db.close()
        except:
            failed_list.append(f"id: {a}, appt: {b}, held: {d}")
    elif d == 'No' or d == 'no' or d == 0 or d == '0':
        if f in ['Address Not Matched','Address Not Geocoded','Lead creation failed','Address Not Created']:
            appts_not_held.append(a)
        try:
            db = mysql.connector.connect(
                            host=CANVASS_HOST,
                            user=CANVASS_USER,
                            passwd=CANVASS_PASSWORD,
                            database=CANVASS_DATABASE
            )
            cursor = db.cursor()
            cursor.execute(f"UPDATE appointments SET held = 0, marked_held_date = '{datetime.date.today()}' WHERE id = {b}")
            db.commit()
            db.close()
        except:
            failed_list.append(f"id: {a}, appt: {b}, held: {d}")
    if c is not None:
        print(c.lower())
        if c.lower() == "multiple contact attempts failed":
            s = 1
        elif c.lower() == "newly signed contract":
            s = 2
        elif c.lower() == "progressing, not signed":
            s = 3
        elif c.lower() == "not interested":
            s = 4
        elif c.lower() == "called/texted" or c == "called/texted #1":
            s = 5
        elif c.lower() == "already solar":
            s = 8
        elif c.lower() == "previously signed, in active pipeline":
            s = 9
        elif c.lower() == "not qualified":
            s = 12
        elif c.lower() == "bad contact info":
            s = 13
        elif c.lower() == "interested, call back after 30 days":
            s = 14
        elif c.lower() == "no show":
            s = 15
        elif c.lower() == "appointment held":
            s = 25
        elif c.lower() == "interested, follow up in 48 hours":
            s = 26
        elif c.lower() == "duplicate":
            s = 29
        elif c.lower() == "non-serviceable area":
            s = 30
        elif c.lower() == "rep rescheduled":
            s = 32
        elif c.lower() == "cancelled":
            s = 34
        elif c.lower() == "rep missed appointment":
            s = 35
        elif c.lower() == "provider to reschedule":
            s = 36
        elif c.lower() == "proposal created":
            s = 37
        elif c.lower() == "provider cancelled":
            s = 38

        if c != 'Not Dispositioned' and c != '' and c != 'not dispositioned':
            print(c)
            
            cursor = db.cursor()
            cursor.execute(f"UPDATE leads SET status_id = {s} WHERE id = {a}")
            db.commit()
            db.close()

        if (e != '') and (b not in ['Address Not Matched','Address Not Geocoded','Lead creation failed','Address Not Created']):
            # SEND NOTE
            url = f"https://api.saleshub.io/company/notes"
            

            headers = {'Content_Type':'application/json',
                  'Authorization':'Bearer {0}'.format(api_key)}

            notes_body = {
                                "address_id": b,
                                "customer_id": f,
                                "employee_id": 14598,
                                "content": f"{e}"
                              }

            notes_response = requests.post(url,data=notes_body,headers=headers)
            notes_response_dict = json.loads(notes_response.text)

        elif(e != '') and (b in ['Address Not Matched','Address Not Geocoded','Lead creation failed','Address Not Created']):
            
            cursor = db.cursor()
            cursor.execute(f"""UPDATE leads SET notes = CONCAT('Admin Note: ',"{e} ",notes) WHERE id = {a}""")
            db.commit()
            db.close()
    else:
        print('Value is a NoneType:', c)
            
print('FAILED HELD UPDATE:\n',failed_list)

df3 = df.loc[(df[3].isin(['Address Not Matched','Address Not Geocoded','Lead creation failed','Address Not Created'])) & (df[14].isin([0,'0',1,'1']))]
df3 = df3[[0,13,14]]
df3 = df3.rename(index=str, columns={0:"id", 13:"appt_held",14:"notes"})
df2 = df2.rename(index=str, columns={0:"id", 1:"appt_held",2:"notes"})
df4 = pd.concat([df2,df3])

service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,range='Canvass Address Failures').execute()
df4 = df4.fillna('')
cols = []
for i in df4.columns:
    cols.append(i)
vals = df4.values.tolist()
values = []
values.append(cols)
for i in vals:
    values.append(i)
    
value_range_body = {
    'majorDimension': 'ROWS',
    'values': values
}

ranges = 'Canvass Address Failures'

service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    valueInputOption='USER_ENTERED',
    range=ranges,
    body=value_range_body
).execute()


# clear the stuff
service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,range="Status Updates").execute()

# Total Volume

cursor = db.cursor()
cursor.execute(f"SELECT l.lat,l.lng,pan.office_id,e.name marketplace_rep,l.id,l.canvass_customer_id,l.canvass_appt_id,p.company_name provider,CONCAT(first_name,' ',last_name) customer,e.phone,e.email,CONCAT(street,' ',city,' ',s.state,' ',zip) address,CONCAT(appt_date,' ',appt_time) marketplace_appt,st.status,l.created,l.opportunity,l.signed,l.canvass_address_id,l.notes FROM leads l LEFT JOIN market_state s ON s.id = l.state_id LEFT JOIN market_status st ON st.id = l.status_id LEFT JOIN (SELECT o.lgcy_id office_id,e.id employee_id FROM employees e LEFT JOIN offices o ON o.id = e.office_id) pan ON pan.employee_id = l.employee_id LEFT JOIN providers p ON p.id = l.provider_id LEFT JOIN employees e ON e.id = l.employee_id WHERE l.created >= '2020-01-01' AND l.employee_id != 1 AND appt_date IS NOT NULL AND l.provider_id = 29")
table_rows = cursor.fetchall()
sh = pd.DataFrame(table_rows,columns=[i[0] for i in cursor.description]) #reps are active
db.close()

pids = list(sh['canvass_address_id'])
pid2 = [i for i in pids if i]
pid_sql = str(pid2)[1:-1]


################ Panel Reps ################

cursor = db.cursor()
cursor.execute("SELECT e.company_email email,e.office_id FROM lgcypower_panel_organization.employees e")
table_rows = cursor.fetchall()
p_reps = pd.DataFrame(table_rows,columns=[i[0] for i in cursor.description])
db.close()

################ Panel Stuff ################

cursor = db.cursor()
cursor.execute("SELECT o.id office_id,o.name office,d.name division,d.id division_id,r.name region,r.id region_id,deal.name dealer FROM lgcypower_panel_organization.offices o LEFT JOIN lgcypower_panel_organization.dealers deal ON deal.id = o.dealer_id LEFT JOIN lgcypower_panel_organization.regions d ON d.id = o.region_id LEFT JOIN lgcypower_panel_organization.divisions r ON r.id = o.division_id")
table_rows = cursor.fetchall()
offices = pd.DataFrame(table_rows,columns=[i[0] for i in cursor.description])
db.close()

panel = pd.merge(p_reps,offices,
                on='office_id',
                how='left',
                indicator=False)


################ Canvass Stuff ################

cursor = db.cursor()
cursor.execute(f"SELECT l.customer_id canvass_customer_id,l.id lead_id,s.status canvass_status,s.created status_timestamp,a.id canvass_appt_id,a.held appt_held,a.marked_held_date,a.number_reschedules,a.start current_appt,a.original_start original_appt,CONCAT(e.first_name,' ',e.last_name) canvass_rep,e.email,e.mobile_phone phone,e.group_id office_id FROM leads l LEFT JOIN (SELECT s.lead_id,ls.name status,s.created FROM lead_lead_statuses s LEFT JOIN lead_statuses ls ON ls.id = s.lead_status_id) s ON s.lead_id = l.id INNER JOIN (SELECT a.*,aa.model_id assigned_employee_id, aa.appointment_id FROM appointment_assignments aa LEFT JOIN appointments a ON a.id = aa.appointment_id WHERE aa.model='employee' AND aa.id = (SELECT MAX(aa2.id) FROM appointment_assignments aa2 WHERE aa2.appointment_id = aa.appointment_id) GROUP BY aa.appointment_id ORDER BY aa.created DESC) a ON a.customer_id = l.customer_id LEFT JOIN employees e ON e.id = a.assigned_employee_id WHERE l.lead_campaign_id = 2 ORDER BY s.created DESC")
table_rows = cursor.fetchall()
c = pd.DataFrame(table_rows,columns=[i[0] for i in cursor.description]) #reps are active
db.close()

# Canvass Notes

cursor = db.cursor()
cursor.execute(f"SELECT address_id canvass_address_id, content canvass_notes,created note_timestamp FROM notes WHERE address_id IN ({pid_sql}) ORDER BY created DESC")
table_rows = cursor.fetchall()
c_notes = pd.DataFrame(table_rows,columns=[i[0] for i in cursor.description]) #reps are active
db.close()

notes = []
cust_notes = c_notes.drop_duplicates(['canvass_address_id'],inplace=False,keep='first')
for i in cust_notes['canvass_address_id']:
    note_history={}
    for (j,k,t) in zip(c_notes['canvass_address_id'],c_notes['canvass_notes'],c_notes['note_timestamp']):
        if i == j and k[:18] != """Call center setter""":
            note_history[str(t.strftime('%m/%d/%Y'))] = f"""{k}"""
    notes.append(note_history)
cust_notes['canvass_notes'] = notes

hist=[]
for i in cust_notes['canvass_notes']:
    j=re.sub("\'",'',str(i))
    p=re.sub(':','-',str(j))
    hist.append(p)
cust_notes['canvass_notes']=hist

canvass = c.drop_duplicates(['lead_id'],inplace=False,keep='first')
canvass['lead_id']=canvass['lead_id'].astype(int)
c['lead_id']=c['lead_id'].astype(int)
history=[]
for i in canvass['lead_id']:
    row_history={}
    for (j,k,t) in zip(c['lead_id'],c['canvass_status'],c['status_timestamp']):
        if i == j:
            row_history[f"{k}"] = f"{t.strftime('%m/%d/%Y')}"
    history.append(row_history)
canvass['status_history'] = history

import re
hist=[]
for i in canvass['status_history']:
    j=re.sub("\'",'',str(i))
    p=re.sub(':','-',str(j))
    hist.append(p)
canvass['status_history']=hist

canvass['appt_held']=canvass['appt_held'].fillna('')
canvass['appt_held']=canvass['appt_held'].astype(str)

canvass['canvass_appt_id'] = canvass['canvass_appt_id'].fillna('')
canvass['canvass_appt_id'] = canvass['canvass_appt_id'].astype(str)
sh['canvass_appt_id'] = sh['canvass_appt_id'].fillna('')
sh['canvass_appt_id'] = sh['canvass_appt_id'].astype(str)
canvass = pd.merge(canvass,sh[['canvass_appt_id','lat','lng']],on='canvass_appt_id',how='left',indicator=False)
true_time = []
for (i,j,x) in zip(canvass['current_appt'],canvass['lat'],canvass['lng']):
    true_time.append(i - datetime.timedelta(hours = 6))
canvass['current_appt'] = true_time

sh['canvass_customer_id'] = sh['canvass_customer_id'].fillna('')
sh['canvass_customer_id'] = sh['canvass_customer_id'].astype(str)
canvass['canvass_customer_id'] = canvass['canvass_customer_id'].astype(str)
sh['status'] = sh['status'].fillna('')
canvass['canvass_status'] = canvass['canvass_status'].fillna('')
canvass = canvass.drop(['canvass_appt_id'],axis=1)
l = pd.merge(sh,canvass,
         on='canvass_customer_id',
         how='left',
         indicator=False)

cust_notes['canvass_address_id'] = cust_notes['canvass_address_id'].fillna('')
cust_notes['canvass_address_id'] = cust_notes['canvass_address_id'].astype(str)
l = pd.merge(l,cust_notes[['canvass_address_id','canvass_notes']],
         on='canvass_address_id',
         how='left',
         indicator=False)

l['canvass_rep']=l['canvass_rep'].fillna('')
l['email_y']=l['email_y'].fillna('')
l['phone_y']=l['phone_y'].fillna('')

rep=[]
em=[]
ph=[]
off=[]
for (a,b,c,d,e,f,g,h) in zip(l['marketplace_rep'],l['email_x'],l['phone_x'],
                             l['canvass_rep'],l['email_y'],l['phone_y'],l['office_id_x'],l['office_id_y']):
    if d != '':
        rep.append(d)
        em.append(e)
        ph.append(f)
        off.append(h)
    else:
        rep.append(a)
        em.append(b)
        ph.append(c)
        off.append(g)
        
l['rep'] = rep
l['email'] = em
l['phone'] = ph
l['panel_office_id'] = off

# l['office_id'] = l['office_id'].astype(int) 
# panel['office_id'] = panel['office_id'].astype(int) 
# l = pd.merge(l,panel,
#              on='office_id',
#              how='left',
#              indicator=False)

l = pd.merge(l,panel,
             on='email',
             how='left',
             indicator=False)

l = l.drop(['marketplace_rep'],axis=1)
l = l.drop(['email_x'],axis=1)
l = l.drop(['phone_x'],axis=1)
l = l.drop(['canvass_rep'],axis=1)
l = l.drop(['email_y'],axis=1)
l = l.drop(['phone_y'],axis=1)
l = l.drop(['office_id_y'],axis=1)

l['appt_held']=l['appt_held'].fillna('')
l['appt_held']=l['appt_held'].astype(str)
l['canvass_status'] = l['canvass_status'].fillna('')
l['status'] = l['status'].fillna('')
where=[]
statii=[]
for (i,j) in zip(l['status'],l['canvass_status']):
    if j != '' and j != 'Created' and j != None:
        where.append('Canvass')
        statii.append(j)
    else:
        where.append('Marketplace')
        statii.append(i)

l['marked_in'] = where
l['current_status'] = statii
#l = l.drop(['canvass_status'],axis=1)
l = l.drop(['status'],axis=1)

made_it=[]
for i in l['canvass_customer_id']:
    if i != '':
        made_it.append('yes')
    else:
        made_it.append('no')
        
l['in_canvass'] = made_it


# l = l.drop(['canvass_customer_id'],axis=1)
# l = l.drop(['lead_id'],axis=1)
# l = l.drop(['status_timestamp'],axis=1)
# l = l.drop(['canvass_employee_id'],axis=1)
# l = l.drop(['canvass_rep'],axis=1)
l['canvass_notes'] = l['canvass_notes'].fillna('')
note_list=[]
og_notes=[]
for (i,j) in zip(l['canvass_notes'],l['notes']):
    og_notes.append(j)
    note_list.append(i)
l['original_notes'] = og_notes
l['additional_notes'] = note_list
l = l.drop(['canvass_notes'],axis=1)
l = l.drop(['notes'],axis=1)

l['current_appt'] = l['current_appt'].fillna('')
appt=[]
for (i,j) in zip(l['marketplace_appt'],l['current_appt']):
    if j != '':
        appt.append(j)
    else:
        appt.append(i)

l['current_appt'] = appt

# ################### Additional Cleanup ####################
# Find past appts
today = datetime.date.today()
todaytime = datetime.datetime.now()
yesterday = datetime.date.today() - datetime.timedelta(1)
yesterdaytime = datetime.datetime.now() - datetime.timedelta(1)
week = datetime.date.today() - datetime.timedelta(7)
weektime = datetime.datetime.now() - datetime.timedelta(7)
past=[]
yestlist=[]
weeklist=[]
tdlist=[]
received=[]

l['current_appt'] = pd.to_datetime(l['current_appt'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
for i in l['current_appt']:
    if i == None or pd.isnull(i):
        past.append(0)
        weeklist.append(0)
    elif type(i) == datetime.date:
        if i < today:
            past.append(1)
        else:
            past.append(0)
        if i < today and i >= week:
            weeklist.append(1)
        else:
            weeklist.append(0)
    else:
        if i < todaytime:
            past.append(1)
        else:
            past.append(0)
        if i < todaytime and i >= weektime:
            weeklist.append(1)
        else:
            weeklist.append(0)
            
l['past'] = past
l['last_week'] = weeklist

l['simple_appt'] = pd.to_datetime(l['current_appt'], format='%Y-%m-%d', errors='coerce')
for (i,j) in zip(l['simple_appt'],l['created']):
    if j.date() == today:
        received.append(1)
    else:
        received.append(0)
    if i == None or pd.isnull(i):
        yestlist.append(0)
        tdlist.append(0)
    else:
        if i.date() == today:
            tdlist.append(1)
        else:
            tdlist.append(0)
        if i.date() == yesterday:
            yestlist.append(1)
        else:
            yestlist.append(0)
            
l['yesterday'] = yestlist
l['today'] = tdlist
l['Received Today'] = received

l=l.loc[l['current_status'] != 'Duplicate']

        
l['original_notes'] = notes

# Update Troubled Dispos
trouble=[]
for i in l['current_status']:
    if i == 'Rep Missed Appointment' or i == '' or i == 'Created':
        trouble.append('Not Dispositioned')
    else:
        trouble.append(i)
l['current_status'] = trouble

# Update Sat/Signed Dispos
signed=[]
for (i,j,k) in zip(l['signed'],l['opportunity'],l['current_status']):
    if i != None:
        signed.append('Newly Signed Contract')
    elif i == None and j != None:
        signed.append('Proposal Created')
    else:
        signed.append(k)
l['current_status'] = signed
        
l['current_status']=l['current_status'].astype(str)
bucket=[]
identifier=[]
for (i,j,k,m) in zip(l['current_status'],l['current_appt'],l['appt_held'],l['past']):
    if i in ('Not Interested','Already Solar','Previously Signed, In Active Pipeline',
             'Not Qualified','Non-Serviceable Area','Cancelled','Provider Cancelled') and k == "" and m == 0:
        bucket.append('Troubled')
        identifier.append('Appt in future and not marked held')
    elif i in ('Not Interested','Already Solar','Previously Signed, In Active Pipeline',
             'Not Qualified','Non-Serviceable Area','Cancelled','Provider Cancelled') and k == "1.0":
        bucket.append('Sat')
        identifier.append('Closed-Lost')
    elif i in ('Not Interested','Already Solar','Previously Signed, In Active Pipeline',
             'Not Qualified','Non-Serviceable Area','Cancelled','Provider Cancelled') and k == "" and m == 1:
        bucket.append('CXL')
        identifier.append('Appt in past and not marked held')
    elif i in ('Not Interested','Already Solar','Previously Signed, In Active Pipeline',
             'Not Qualified','Non-Serviceable Area','Cancelled','Provider Cancelled') and k == "0.0":
        bucket.append('CXL')
        identifier.append('Closed-Lost')
        
    elif i in ('Newly Signed Contract','Newly Signed'):
        bucket.append('Sat')
        identifier.append('Closed-Won')
        
    elif i in ('Proposal Created','Progressing, Not Signed','Called/Texted','Interested, Call Back After 30 Days',
              'Interested, Follow Up in 48 Hours','Rep Rescheduled','Rescheduled by Customer') and k == "" and m == 0:
        bucket.append('Pending')
        identifier.append('Status not Terminal')
    elif i in ('Proposal Created','Progressing, Not Signed','Called/Texted','Interested, Call Back After 30 Days',
              'Interested, Follow Up in 48 Hours','Rep Rescheduled','Rescheduled by Customer') and k == "1.0":
        bucket.append('Sat')
        identifier.append('Status not Terminal')
    elif i in ('Proposal Created','Progressing, Not Signed','Called/Texted','Interested, Call Back After 30 Days',
              'Interested, Follow Up in 48 Hours','Rep Rescheduled','Rescheduled by Customer') and k == "" and m == 1:
        bucket.append('Troubled')
        identifier.append('Appt in past and not marked held')
    elif i in ('Proposal Created','Progressing, Not Signed','Called/Texted','Interested, Call Back After 30 Days',
              'Interested, Follow Up in 48 Hours','Rep Rescheduled','Rescheduled by Customer') and k == "0.0":
        bucket.append('Troubled')
        identifier.append('Positive Status, not marked held')
        
    elif i in ('Multiple Contact Attempts Failed','Multiple Contact Attempts','Bad Contact Info',
               'No Show','Provider to Reschedule') and k == "" and m == 0:
        bucket.append('RXL')
        identifier.append('Status not Terminal')
    elif i in ('Multiple Contact Attempts Failed','Multiple Contact Attempts','Bad Contact Info',
               'No Show','Provider to Reschedule') and k == "1.0":
        bucket.append('Sat')
        identifier.append('Status conflicts with held status')
    elif i in ('Multiple Contact Attempts Failed','Multiple Contact Attempts','Bad Contact Info',
               'No Show','Provider to Reschedule') and k == "" and m == 1:
        bucket.append('RXL')
        identifier.append('Appt in past and not marked held')
    elif i in ('Multiple Contact Attempts Failed','Multiple Contact Attempts','Bad Contact Info',
               'No Show','Provider to Reschedule') and k == "0.0":
        bucket.append('RXL')
        identifier.append('Status not Terminal')
        
    elif i == 'Not Dispositioned' or i == 'Expired':
        bucket.append('Troubled')
        identifier.append('Not Dispositioned by Rep')
        
    elif i in ('Appointment Held w/ Bill','Appointment Held','Appointment Held w/ Bill-Zoom') and k == "" and m == 0:
        bucket.append('Sat')
        identifier.append('Not Dispositioned in Canvass')
    elif i in ('Appointment Held w/ Bill','Appointment Held','Appointment Held w/ Bill-Zoom') and k == "1.0":
        bucket.append('Sat')
        identifier.append('Not Dispositioned in Canvass')
    elif i in ('Appointment Held w/ Bill','Appointment Held','Appointment Held w/ Bill-Zoom') and k == "" and m == 1:
        bucket.append('Sat')
        identifier.append('Not Dispositioned in Canvass')
    elif i in ('Appointment Held w/ Bill','Appointment Held','Appointment Held w/ Bill-Zoom') and k == "0.0":
        bucket.append('Sat')
        identifier.append('Not Dispositioned in Canvass')
        
    elif 'Called/Texted' in i and k == "" and m == 0:
        bucket.append('RXL')
        identifier.append('Status not Terminal')
    elif 'Called/Texted' in i and k == "1.0":
        bucket.append('Sat')
        identifier.append('Status conflicts with held status')
    elif 'Called/Texted' in i and k == "" and m == 1:
        bucket.append('RXL')
        identifier.append('Appt in past and not marked held')
    elif 'Called/Texted' in i and k == "0.0":
        bucket.append('RXL')
        identifier.append('Status not Terminal')
        
    else:
        print(i,j,k,m)
    

l['appointment_outcome'] = bucket
l['to do'] = identifier
cutoff = datetime.datetime(2021, 4, 6)
identifier_2=[]
for (i,j,k) in zip(l['to do'],l['canvass_status'],l['created']):
    if j in ('Rep Missed Appointment','') and k < cutoff:
        identifier_2.append('Not Dispositioned')
    elif k < cutoff:
        identifier_2.append('Prior to Canvass Integration')
    elif i != 'Not Dispositioned by Rep' and j == '' and k >= cutoff:
        identifier_2.append('Not Dispositioned in Canvass')
    else:
        identifier_2.append(i)

l['to do'] = identifier_2


#Drop cols
l = l.drop(['lead_id'],axis=1)
l = l.drop(['opportunity'],axis=1)
l = l.drop(['canvass_status'],axis=1)
l = l.drop(['status_timestamp'],axis=1)
l = l.drop(['marked_held_date'],axis=1)
l = l.drop(['number_reschedules'],axis=1)
l = l.drop(['original_appt'],axis=1)
l = l.drop(['marketplace_appt'],axis=1)
# ####################### Summary #######################
# disp = l.drop_duplicates('current_status',keep='first')
# count_list = []
# for i in disp['current_status']:
#     count = 0
#     for j in l['current_status']:
#         if i == j:
#             count += 1
#     count_list.append(count)
# disp['count'] = count_list
# disp = disp[['current_status','count']]
# disp = disp.sort_values(by=['count'],ascending=False)
pd.set_option('display.max_rows', 1000)

cur_appt = []
for i in l['current_appt']:
    if pd.isnull(i):
        cur_appt.append(None)
    else:
        cur_appt.append(i.strftime('%m/%d/%Y %I:%M %p'))
l['current_appt'] = cur_appt

# appts=[]
# for i in l['current_appt']:
#     try:
#         appts.append(i.strftime('%m/%d/%Y %I:%M %p'))
#     except:
#         appts.append(i)
# l['current_appt'] = appts

l['current_appt'] = l['current_appt'].fillna('')
l['current_appt'] = l['current_appt'].astype(str)

created = []
for i in l['created']:
    if pd.isnull(i):
        created.append(None)
    else:
        created.append(i.strftime("%m/%d/%Y"))
l['created'] = created

signed = []
for i in l['signed']:
    if pd.isnull(i):
        signed.append(None)
    else:
        signed.append(i.strftime("%m/%d/%Y"))
l['signed'] = signed

l2 = l[['id','rep','canvass_customer_id','canvass_address_id','canvass_appt_id',
        'phone', 'email','office','panel_office_id',
       'region','region_id','division','division_id','dealer',
       'provider', 'customer','address', 
       'created', 'current_appt','appt_held','current_status',
        'marked_in', 'in_canvass','today','past','signed',
       'yesterday','last_week','appointment_outcome', 'to do',
        'original_notes','additional_notes','Received Today']]

l2['region_id'] = l2['region_id'].fillna(0)
l2['region_id'] = l2['region_id'].astype(int)
l2['division_id'] = l2['division_id'].fillna(0)
l2['division_id'] = l2['division_id'].astype(int)

admin_notes=[]
for i in l2['id']:
    admin_notes.append('')
l2['Admin Notes'] = admin_notes

action=[]
for (i,j,k) in zip(l2['marked_in'],l2['current_status'],l2['appt_held']):
    if j == 'Not Dispositioned' and k == '':
        action.append('Update Held/Not Held AND Status in Canvass')
    elif j == 'Not Dispositioned':
        action.append('Update Status')
    elif k == '':
        action.append('Update Held/Not Held in Canvass')
    else:
        action.append('Correctly Dispositioned')
l2['Action'] = action

l2 = l2.loc[(l2['provider'] == 'Safe Haven Security')]

# All time SH
l6 = l2[['id','canvass_address_id','canvass_customer_id','canvass_appt_id','rep', 'phone', 'email','office','provider','customer','address','current_appt','current_status','appt_held','signed','Action','original_notes','additional_notes']]
l6 = l6.loc[(l6['provider'] == 'Safe Haven Security')]
l6 = l6.rename(index=str, columns={'rep':"Rep", 'phone':"Rep Phone", 'email':"Rep Email",'office':'Rep Office','provider':'Lead Provider','customer':'Customer','address':'Customer Address','current_appt':'Appointment','current_status':'Status','Action':'Action Needed'})

l2 = l2.sort_values(by=['rep'],ascending=True)
l2 = l2.drop_duplicates(['id'],keep='first')

# Today's appts
l5 = l2.loc[l2['today'] == 1]
l5 = l5[['id','canvass_address_id','canvass_customer_id','canvass_appt_id','rep', 'phone', 'email','office','provider','customer','address','current_appt','current_status','appt_held','signed','Action','original_notes','additional_notes']]
l5 = l5.rename(index=str, columns={'rep':"Rep", 'phone':"Rep Phone", 'email':"Rep Email",'office':'Rep Office','provider':'Lead Provider','customer':'Customer','address':'Customer Address','current_appt':'Appointment','current_status':'Status','Action':'Action Needed'})

l7 = l2.loc[l2['Received Today'] == 1]
l7 = l7[['id','canvass_address_id','canvass_customer_id','canvass_appt_id','rep', 'phone', 'email','office','provider','customer','address','current_appt','current_status','appt_held','signed','Action','original_notes','additional_notes']]
l7 = l7.rename(index=str, columns={'rep':"Rep", 'phone':"Rep Phone", 'email':"Rep Email",'office':'Rep Office','provider':'Lead Provider','customer':'Customer','address':'Customer Address','current_appt':'Appointment','current_status':'Status','Action':'Action Needed'})

# Get rid of future appts
l2=l2.loc[l2['past'] == 1]

# Past appts incorrect dispo
l4 = l2.loc[l2['Action'] != 'Correctly Dispositioned']
l4 = l4[['id','canvass_address_id','canvass_customer_id','canvass_appt_id','rep', 'phone', 'email','office','provider','customer','address','current_appt','current_status','appt_held','Admin Notes','signed','Action','original_notes','additional_notes']]
l4 = l4.rename(index=str, columns={'rep':"Rep", 'phone':"Rep Phone", 'email':"Rep Email",'office':'Rep Office','provider':'Lead Provider','customer':'Customer','address':'Customer Address','current_appt':'Appointment','current_status':'Status','Action':'Action Needed'})

# Yesterday's appts
l3 = l2.loc[l2['yesterday'] == 1]
l3 = l3[['id','canvass_address_id','canvass_customer_id','canvass_appt_id','rep', 'phone', 'email','office','provider','customer','address','current_appt','current_status','appt_held','signed','Action','original_notes','additional_notes']]
l3 = l3.rename(index=str, columns={'rep':"Rep", 'phone':"Rep Phone", 'email':"Rep Email",'office':'Rep Office','provider':'Lead Provider','customer':'Customer','address':'Customer Address','current_appt':'Appointment','current_status':'Status','Action':'Action Needed'})

# All time SH
l6 = l2[['id','canvass_address_id','canvass_customer_id','canvass_appt_id','rep', 'phone', 'email','office','provider','customer','address','current_appt','current_status','appt_held','signed','Action','original_notes','additional_notes']]
l6 = l6.loc[(l6['provider'] == 'Safe Haven Security')]
l6 = l6.rename(index=str, columns={'rep':"Rep", 'phone':"Rep Phone", 'email':"Rep Email",'office':'Rep Office','provider':'Lead Provider','customer':'Customer','address':'Customer Address','current_appt':'Appointment','current_status':'Status','Action':'Action Needed'})

l = l[['rep', 'phone', 'email','office', 'region','division','dealer',
       'provider', 'customer','address', 
       'created', 'current_appt','appt_held','current_status',
       'status_history', 'marked_in', 'in_canvass','canvass_address_id',
       'yesterday', 'appointment_outcome', 'to do','original_notes','additional_notes']]
#upload(dbx,l,'theDeanTeam','undispositioned_leads.csv', overwrite=True)

# add the stuff
l4 = l4.fillna('')
cols = []
for i in l4.columns:
    cols.append(i)
vals = l4.values.tolist()
values = []
values.append(cols)
for i in vals:
    values.append(i)
    
value_range_body = {
    'majorDimension': 'ROWS',
    'values': values
}

ranges = 'Status Updates'

service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    valueInputOption='USER_ENTERED',
    range=ranges,
    body=value_range_body
).execute()


service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,range="Yesterday's Appts").execute()
l3 = l3.fillna('')
cols = []
for i in l3.columns:
    cols.append(i)
vals = l3.values.tolist()
values = []
values.append(cols)
for i in vals:
    values.append(i)
    
value_range_body = {
    'majorDimension': 'ROWS',
    'values': values
}

ranges = "Yesterday's Appts"

service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    valueInputOption='USER_ENTERED',
    range=ranges,
    body=value_range_body
).execute()

service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,range="Today's Appts").execute()
l5 = l5.fillna('')
cols = []
for i in l5.columns:
    cols.append(i)
vals = l5.values.tolist()
values = []
values.append(cols)
for i in vals:
    values.append(i)
    
value_range_body = {
    'majorDimension': 'ROWS',
    'values': values
}

ranges = "Today's Appts"

service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    valueInputOption='USER_ENTERED',
    range=ranges,
    body=value_range_body
).execute()

service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,range="").execute()
l6 = l6.fillna('')
cols = []
for i in l6.columns:
    cols.append(i)
vals = l6.values.tolist()
values = []
values.append(cols)
for i in vals:
    values.append(i)
    
value_range_body = {
    'majorDimension': 'ROWS',
    'values': values
}


service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    valueInputOption='USER_ENTERED',
    range=ranges,
    body=value_range_body
).execute()

service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,range="Received Today").execute()
l7 = l7.fillna('')
cols = []
for i in l7.columns:
    cols.append(i)
vals = l7.values.tolist()
values = []
values.append(cols)
for i in vals:
    values.append(i)
    
value_range_body = {
    'majorDimension': 'ROWS',
    'values': values
}
ranges = 'Received Today'

service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    valueInputOption='USER_ENTERED',
    range=ranges,
    body=value_range_body
).execute()

