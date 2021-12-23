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

def getEmailContent(event, value, timestamp):
    #Kindly change templateFilePath using needed path
    templateFilePath = "EmailTemplate.html"
    soup = BeautifulSoup(open(templateFilePath),"html.parser")
    content = soup.find(class_='content-row')
        
    eventRow = BeautifulSoup("""<tr><td>1</td><td>"""+event+"""</td><td>"""+str(value)+"""</td><td>The value are below the treshold. Please prepare for the Action plan.</td></tr>\n""","html.parser")

    content.insert(1, eventRow)

    return soup

def GetRows(source):
    sql = "Select * From " + source
    return pd.read_sql(sql, connStr) 

def CheckValue(values, _trigger, kri_type):
    if kri_type == 'Higher Better':
        return float(values[len(values)-1]) <= _trigger
    else:
        return float(values[len(values)-1]) >= _trigger

#TODO : update sender and recipient method
def TriggerSMTP(kri_desc, value, timestamp):
    content = getEmailContent(kri_desc, value, timestamp)
    sendEmail("ermwebui.email@gmail.com", "Jakarta1!", "iqbal.lukman@pwc.com", content)

if __name__ == '__main__':
    # connStr = pyodbc.connect('''DRIVER={FreeTDS}; \
    #                             SERVER=host.docker.internal; \
    #                             PORT=57155; \
    #                             UID=erm; \
    #                             PWD=Jakarta1!; \
    #                             DATABASE=ERM; 
    #                             Trusted_Connection=yes''')

    connStr = pyodbc.connect('''DRIVER={ODBC Driver 13 for SQL Server}; \
                                SERVER=IDPF1RNMKZ/PROD17,57155; \
                                UID=sa; \
                                PWD=Jakarta1!; \
                                DATABASE=ERM; 
                                Trusted_Connection=yes''')

    selection = GetRows('KRISelection')
    setup = GetRows('KRISetup')

    selection = selection.merge(setup, how='left', on='kri_id')
    selection = selection.dropna()

    for index,row in selection.iterrows():
        view = GetRows(row['kri_id'])
        print(view)
        view = view[view['prediction'] == 'No']

        #TODO : dynamic target variable
        if(CheckValue(view['price'].values, row['action_plan_trigger_amount'], row['kri_type'])):
            TriggerSMTP(row['kri_desc_x'], view['price'].values[len(view['co_price'].values)-1], datetime.datetime.now().strftime("%d %B, %Y"))