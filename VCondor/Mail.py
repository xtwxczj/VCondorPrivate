#!/usr/bin/env python
import smtplib
from email.mime.text import MIMEText
from email.header import Header

def sendTextMail(subject,content):
    sender = 'vcondor@163.com'
    receiver = ['chengzj@ihep.ac.cn']
    smtpserver = 'smtp.163.com'
    username = 'vcondor'
    password = 'VCondor123456'

    msg = MIMEText(content,'text','utf-8')
    msg['Subject'] = Header(subject, 'utf-8')

    message = """\From: %s\nTo: %s\nSubject: %s\n\n%s""" % (sender, ", ".join(receiver), subject, content)

    smtp = smtplib.SMTP()
    smtp.connect('smtp.163.com')
    smtp.ehlo()
    smtp.starttls()
    smtp.login('vcondor@163.com','VCondor123456')
    smtp.sendmail(sender, receiver, message)
    smtp.quit()






if __name__ == '__main__':
    sendTextMail('text','hello')

