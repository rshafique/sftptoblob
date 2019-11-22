

# Email Sending Function

import smtplib


from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders



def send_email(fromaddr,toaddr,subject,body):

    msg = MIMEMultipart()

    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = subject

    body = body

    msg.attach(MIMEText(body, 'plain'))

    # filename = "HSE_NEW_INCIDENTS.csv"
    # attachment = open(
    #     "C:/Users/raihan.shafique/Documents/Automated Report Generation/20180920 HSE Incident Auto Generated Email Python/HSE_NEW_INCIDENTS.csv",
    #     "rb")

    # part = MIMEBase('application', 'octet-stream')
    # part.set_payload((attachment).read())
    # encoders.encode_base64(part)
    # part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

    # msg.attach(part)

    server = smtplib.SMTP('smtp.office365.com', 587)
    server.starttls()
    server.login(fromaddr, 'Dgregdgre!1')
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr.split(','), text)
    server.quit()


# Error Loggin on the CSV File

# A CSV file named ErrorLog need to be in the root folder of the project

import csv
import datetime


def error_log(error_msg):
    with open('ErrorLog.csv', mode='w') as employee_file:
        employee_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        DateTime = (datetime.datetime.now())
        employee_writer.writerow([DateTime, error_msg])


