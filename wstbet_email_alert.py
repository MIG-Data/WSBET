# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 20:40:19 2021

@author: tcai
"""

import requests
import json
import datetime
try:
   from urllib.parse import urlencode, quote_plus
except ImportError:
   from urlparse import urlparse
   from urllib import urlencode
import sys

import re
import matplotlib.pyplot as plt
import os
import pyodbc

import datetime
from pytz import timezone
import pytz
import pandas as pd
import smtplib, ssl
import smtplib  
import email.utils
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase



sender_email = "tcai@migcap.com"
receiver_email = "gtxskyline36@gmail.com"

subject_email = 'Reddit WSB ticker mention alert'

SENDER = sender_email 
SENDERNAME = 'Tianjing'


# Replace smtp_username with your Amazon SES SMTP user name.
USERNAME_SMTP = "AKIAWYG6AKIPZUZARZ73"

# Replace smtp_password with your Amazon SES SMTP password.
PASSWORD_SMTP = "BJ6UDl0zHSYMMeHMtDUHQVJws+qqxWfkZwVJwH+7IDUS"

# (Optional) the name of a configuration set to use for this message.
# If you comment out this line, you also need to remove or comment out
# the "X-SES-CONFIGURATION-SET:" header below.
#CONFIGURATION_SET = "ConfigSet"

# If you're using Amazon SES in an AWS Region other than US West (Oregon), 
# replace email-smtp.us-west-2.amazonaws.com with the Amazon SES SMTP  
# endpoint in the appropriate region.
HOST = "email-smtp.us-east-2.amazonaws.com"
PORT = 587


receipients = ["achan@migcap.com","bporter@migcap.com","echung@migcap.com","gbockenek@migcap.com","nprakash@migcap.com","rmerage@migcap.com", "jlewis@migcap.com", "rtyler@migcap.com","yjeon@migcap.com","tcai@migcap.com"]

date_format='%m/%d/%Y %H:%M:%S'
date = datetime.datetime.now().astimezone(timezone('US/Pacific'))
pstDateTime = date.strftime(date_format)


name_email_dict = {'NP': 'nprakash@migcap.com', 'PRAKASH': 'nprakash@migcap.com', 'CHUNG': 'echung@migcap.com',\
                   'EC': 'echung@migcap.com', 'GB': 'gbockenek@migcap.com', 'BOCKENEK': 'gbockenek@migcap.com', \
                   'BP': 'bporter@migcap.com', 'PORTER': 'bporter@migcap.com', 'RM': 'RMerage@migcap.com', \
                   'MERAGE': 'RMerage@migcap.com', 'AC':'achan@migcap.com'}

name_dict = {'NP': 'Nitin', 'PRAKASH': 'Nitin', 'BP': 'Bryan', 'PORTER': 'Bryan', 'CHUNG': 'Edwin', 'EC': 'Edwin', 'GB': 'Garrett', 'BOCKENEK': 'Garrett', \
             'RM': 'Richard', 'MERAGE': 'Richard', 'AC':'Aaron'}

def getEmailContent(first_name, ticker, ticker_count, data_html):
    global pstDateTime
    
    email_content ="""
                    Hi """+first_name+""",
                    <br><br>
                    As of {0} the ticker {1} has {2} mentions among today's all WSB posts. Please be aware of potential market movement. Detailed info can be visualized at dashboard on company portal: https://www.migcap-portal.com/wsb/ \
                    (be sure to change the date filter and click 'Refresh button' at bottom of page to get most up to date data). Below are the top 10 most popular ticker related comments and posts.
                    <br><br>""".format(str(pstDateTime), ticker, ticker_count) 
    email_content = email_content + """<br><br>""" +'<style>#table1 {border-spacing: 2; border-collapse: collapse; font-family:arial;font-size:14px;width:90%;} #table1 td:last-child {width:60;} #table1 td, th {border: 1px solid black; align: center;}</style>' + \
    data_html + '<div style="max-width:580px; margin:1 auto;"><p></p></div>' + "<br><br>"+ """Regards,<br>Tianjing"""

    return email_content

def get_conn():
    #conn = pyodbc.connect(server='tcp:bc5756vevd.database.windows.net,1433;Database=GUEST', user='guest_test', passwd='mig_auth5',charset='utf8')
    #conn = pyodbc.connect('DRIVER=lib\libmsodbcsql-13.so;SERVER=bc5756vevd.database.windows.net,1433;DATABASE=MIG;UID=garquette;PWD=Working1!')
    conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=bc5756vevd.database.windows.net,1433;DATABASE=MIG;UID=garquette;PWD=Working1!')
    return conn

def insert(cur, sql, args):
    cur.execute(sql, args)   

def insert_data( table, data, schema = 'WSTBET'):
    with get_conn() as conn:
        with conn.cursor() as cur:
            # insert product info data
            
            sql = 'insert into ' + schema + '.' + table + ' values(?' + ', ?' * (len(data)-1)  +')'
            #print(data)
            try:
                insert(cur, sql=sql, args= data)
        
            except pyodbc.IntegrityError as err: # use integrityerror to avoid inserting duplicate records
                print(err)
            
            except pyodbc.DataError as data_err: 
                print(data_err)
            
            except pyodbc.ProgrammingError as prog_err:
                print(prog_err)
    return

def check_alert():
    global name_dict, name_email_dict, pstDateTime, receipients
    pstDateTime_dt = datetime.datetime.strptime(pstDateTime, date_format)
    current_date = str(datetime.datetime.now().date())
    count_threshold = 5
    analyst_ticker_count = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            get_data = '''select *
                            from(
                            select Analyst, Position, ticker, count(distinct comment_id) as ticker_count 
                            from (
                                    select *
                                    from WSTBET.reddit_streaming
                                    where CAST(created_utc as DATE) = '{0}') R inner join dbo.daily_portfolio_report D on R.ticker = D.SecurityID
                                    where D.SecurityID NOT IN ('TV', 'RH')
                                    GROUP BY Analyst, Position, ticker) analyst_ticker
                            where analyst_ticker.ticker_count >= {1}
                        '''.format(current_date, count_threshold)
            #print(get_data)
            cur.execute(get_data)
            analyst_ticker_count = [row for row in cur]
    
    for record in analyst_ticker_count:
        analyst = record[0]
        
        position = record[1]
        ticker = record[2]
        ticker_count = record[3]
        try:
            with get_conn() as conn:
                with conn.cursor() as cur1:
                    get_alert_record = '''select analyst, ticker, max(alert_datetime) as last_alert_time, max(comment_num) as last_comment_num from WSTBET.alert_record
                                            where CAST(alert_datetime as DATE) = '{0}' AND analyst = '{1}' AND ticker = '{2}' GROUP BY analyst, ticker'''.format(str(pstDateTime_dt.date()), analyst, ticker)
                    #print(get_data)
                    cur1.execute(get_alert_record)
                    alert_records = cur1.fetchall()
        except:
            alert_records = []
            
        if alert_records == []:  
            data_html = fetch_data_html(position, ticker, current_date)
            email_content = getEmailContent(name_dict[analyst], ticker, ticker_count, data_html)
            '''
            o = win32com.client.Dispatch("Outlook.Application")
    
            Msg = o.CreateItem(0)
            Msg.Importance = 0
            Msg.Subject = 'Reddit WSB ticker mention alert'
            Msg.HTMLBody = email_content
            
            #Msg.To = ', '.join([name_email_dict[analyst]])
            Msg.To = ','.join([ receiver_email])
            #Msg.CC = STRING_CONTAINING_CC
            #Msg.BCC = STRING_CONTAINING_BCC
            
            Msg.SentOnBehalfOfName = sender_email
            Msg.ReadReceiptRequested = True
            Msg.OriginatorDeliveryReportRequested = True
            
            Msg.Send()
            '''
            
            # Create message container - the correct MIME type is multipart/alternative.
            RECIPIENT = ','.join([name_email_dict[analyst], receiver_email])
            msg = MIMEMultipart('alternative')
            msg['Subject'] = 'Reddit WSB ticker mention alert'
            msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
            msg['To'] = RECIPIENT
            # Comment or delete the next line if you are not using a configuration set
            #msg.add_header('X-SES-CONFIGURATION-SET',CONFIGURATION_SET)
            
            # Record the MIME types of both parts - text/plain and text/html.
            
            part2 = MIMEText(email_content, 'html')
            
            # Attach parts into message container.
            # According to RFC 2046, the last part of a multipart message, in this case
            # the HTML message, is best and preferred.
            
            msg.attach(part2)
            
            # Try to send the message.
            try:  
                server = smtplib.SMTP(HOST, PORT)
                server.ehlo()
                server.starttls()
                #stmplib docs recommend calling ehlo() before & after starttls()
                server.ehlo()
                server.login(USERNAME_SMTP, PASSWORD_SMTP)
                server.sendmail(SENDER, RECIPIENT, msg.as_string())
                server.close()
            # Display an error message if something goes wrong.
            except Exception as e:
                print ("Error: ", e)
            else:
                print('Ticker alert for' + ticker + ' with updated comment number ' + str(ticker_count) + ' sent to ' + name_email_dict[analyst])
            
            insert_data('alert_record', tuple([pstDateTime_dt, analyst, position, ticker, ticker_count]), 'WSTBET')
            

def fetch_data_html(position, ticker, current_date):
     with get_conn() as conn:
            get_comment_data =  '''select  ticker, created_utc, comment_author, comment_body, comment_ups,  submission_ups, submission_permalink
                                from WSTBET.reddit_streaming
                                where ticker = '{0}' AND CAST(created_utc as DATE) = '{1}'
                                order by submission_ups desc, created_utc desc'''.format(ticker, current_date)
            comment_df = pd.read_sql(get_comment_data, conn)
            comment_df = comment_df.iloc[:10]
            html = '<table id="table1">'
            th = "<tr>"
            th += "<th>{0}</th>".format('Position')
            for col in comment_df.columns:
                th += "<th>{0}</th>".format(col)
            th += "</tr>"
            html += th
            html_row = None
            for index, row in comment_df.iterrows():
                row1 = """<tr style='border-top: double;'>
                <td style='text-align:center;'>{0}</td><td style='text-align:center;'>{1}</td><td style='text-align:center;'>{2}</td><td style='text-align:center;'>{3}</td><td>{4}</td><td style='text-align:center;'>{5}</td>
                <td style='text-align:center;'>{6}</td><td style='text-align:center;'>{7}</td>
                </tr>""".format(position,  row["ticker"], row["created_utc"], row["comment_author"], row["comment_body"][:100], int(row["comment_ups"]), int(row["submission_ups"]), row["submission_permalink"]) 
                html_row = row1 + '<br>'
                html = html + html_row
            html = html + "</table>"
            return html
        
def main():
    check_alert()
    

if __name__ == '__main__':
    main() 
