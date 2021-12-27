import pyodbc
import pandas as pd
import sys
import time
import datetime
from pytz import timezone
import dateutil.relativedelta
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from bs4 import BeautifulSoup
import urllib


def sendEmail(sender, password, recipient, content):
    message = MIMEMultipart("alternative")
    message["Subject"] = "Test Email"
    message["From"] = sender
    message["To"] = recipient

    part2 = MIMEText(content, "html")

    message.attach(part2)

    with open('logo_pwc.png', 'rb') as f:
        # set attachment mime and file name, the image type is png
        mime = MIMEBase('image', 'png', filename='logo_pwc.png')
        # add required header data:
        mime.add_header('Content-Disposition', 'attachment', filename='logo_pwc.png')
        mime.add_header('X-Attachment-Id', '0')
        mime.add_header('Content-ID', '<0>')
        # read attachment file content into the MIMEBase object
        mime.set_payload(f.read())
        # encode with base64
        encoders.encode_base64(mime)
        # add MIMEBase object to MIMEMultipart object
        message.attach(mime)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender, password)
        server.sendmail(
            sender, recipient, message.as_string()
        )
        print("email sent to " + str(recipient))

def getEmailContent(value, timestamp):
    #Kindly change templateFilePath using needed path
    templateFilePath = "EmailTemplate.html"
    soup = BeautifulSoup(open(templateFilePath),"html.parser")
    content = soup.find(class_='content-row')
    
    for index,row in value.iterrows():
        view = GetRows(row['kri_id'])
        print(view)
        eventRow = BeautifulSoup("""<tr><td>1</td><td>"""+row['volume']+"""</td><td>"""+row['city']+"""</td><td>"""+row['timestamp']+"""</td></tr>\n""","html.parser")
        content.insert(1, eventRow)

    return soup

def GetRows(source):
    sql = "Select TOP 10 * From " + source + "ORDER BY timestamp DESC"
    return pd.read_sql(sql, connStr) 

def CheckValue(values, _trigger, kri_type):
    if kri_type == 'Higher Better':
        return float(values[len(values)-1]) <= _trigger
    else:
        return float(values[len(values)-1]) >= _trigger

#TODO : update sender and recipient method
def TriggerSMTP(value, timestamp):
    content = getEmailContent(value, timestamp)
    sendEmail("ermwebui.email@gmail.com", "Jakarta1!", "muhiqballukman@gmail.com", content)

if __name__ == '__main__':
    # connStr = pyodbc.connect('''DRIVER={FreeTDS}; \
    #                             SERVER=host.docker.internal; \
    #                             PORT=57155; \
    #                             UID=erm; \
    #                             PWD=Jakarta1!; \
    #                             DATABASE=ERM; 
    #                             Trusted_Connection=yes''')

    connStr = pyodbc.connect('''DRIVER={ODBC Driver 13 for SQL Server}; \
                                SERVER=IDFP1RNMKZ\PROD2017,57155; \
                                UID=sa; \
                                PWD=Jakarta1!; \
                                DATABASE=ERM_datamart; 
                                Trusted_Connection=yes''')

    data = GetRows('CrudeOil')
    data = data.dropna()

    for index,row in data.iterrows():
        view = GetRows(row['timestamp'])
        print(view)
    
    TriggerSMTP(data,datetime.datetime.now().strftime("%d %B, %Y"))