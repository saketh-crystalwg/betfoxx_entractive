import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from babel.numbers import format_currency
import numpy as np

import mysql.connector
from mysql.connector import Error

from sqlalchemy import create_engine

import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

def send_mail(send_from, send_to, subject, text, server, port, username='', password='', filename=None):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    if filename is not None:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(filename, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={filename}')
        msg.attach(part)

    smtp = smtplib.SMTP_SSL(server, port)
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

engine = create_engine('postgresql://orpctbsqvqtnrx:530428203217ce11da9eb9586a5513d0c7fe08555c116c103fd43fb78a81c944@ec2-34-202-53-101.compute-1.amazonaws.com:5432/d46bn1u52baq92',\
                           echo = False)

daily_mailer = pd.read_sql_query('''with en_base as ( \
select *, CASE WHEN "Country" = 'Finland' then right(cast("mobile_number" as varchar) , 6) \
              WHEN "Country" = 'UK' then right(cast("mobile_number" as varchar), 7) end as mobile_req from "Entractive_cross_sale_base"),
txn_base as (
select  "ClientId", "Email", min(date("CreationTime"))  as targetdepositdate from public.customer_transactions_betfoxx
where "Status" in ('Approved', 'ApprovedManually')
and "Type" in (2,3)
GROUP BY 1 ,2),

bf_cust as  (
SELECT "Id", "Email", "MobileNumber", right("MobileNumber", 6) as mobile_6, right("MobileNumber", 7) as mobile_7, date("CreationTime") as targetregistrationdate  from customers_betfoxx),

mapping as (
SELECT a.*, case when b."Id" is not  null then  b."Id"
when c."Id"  is not null then c."Id"
when d."Id" is not  null then d."Id"  end as "TargetClientUserId" , 
case when b."Id" is not  null then 'email_mapping'
when c."Id" is not null or d."Id" is not null then 'phone_mapping' end as mapping_logic
FROM en_base AS a 
LEFT JOIN bf_cust AS b 
ON a."email" = b."Email"
left join bf_cust as  c  
on a.mobile_req = c.mobile_7
left join bf_cust as d 
on a.mobile_req = d.mobile_6),

mapping_crossed  as (
select a.*, b.targetdepositdate, c.targetregistrationdate from mapping as a 
left join txn_base  as b 
on a."TargetClientUserId" = b."ClientId"
left join bf_cust as c
on a."TargetClientUserId" = c."Id"
)

select distinct "User ID"  as OriginalClientUserId, case when "brand_name" is not null then "brand_name"  else '77Spins' end  as OriginalBrandId, 'BetFoxx' as "targetBrand",
case when "last_deposit_date" is not null then "last_deposit_date" else '2024-01-01' end as last_deposit_date,
"TargetClientUserId","targetregistrationdate", "targetdepositdate",  "email_consent", "sms_consent" , "mapping_logic"
 from mapping_crossed''', con= engine)

date = dt.datetime.today()-  timedelta(1)
date_1 = date.strftime("%m-%d-%Y")
filename = f'Entractive_Betfoxx_Daily_list_{date_1}.xlsx'
sub = f'Entractive Betfoxx Daily Check {date_1}'

with pd.ExcelWriter(filename, engine='openpyxl') as writer:
    daily_mailer.to_excel(writer, sheet_name="Daily_Check", index=False)

subject = sub
body = f"Hi,\n\nAttached contains the list of customers part of Entractive campaigns for the {date_1} for Betfoxx. \n\nThanks,\nSaketh"
sender = "sakethg250@gmail.com"
recipients = ["sebastian@crystalwg.com","sakethg250@gmail.com","saketh@crystalwg.com","alin@crystalwg.com"]
password = "xjyb jsdl buri ylqr"
send_mail(sender, recipients, subject, body, "smtp.gmail.com", 465, sender, password, filename)
