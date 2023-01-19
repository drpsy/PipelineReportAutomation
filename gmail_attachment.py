import os
import traceback
from utils import EMAIL_INFO, MANAGER_MAIL_LIST, SAlE_MAIL_LIST,OPERATOR_MAIL_LIST, createDirectory
import poplib
import smtplib
import easyimap
import re
from datetime import date
from imap_tools import MailBox, AND, OR
from datetime import timedelta, datetime
from datetime import datetime

# import easyimap
# enable less secure apps on your google account
# https://myaccount.google.com/lesssecureapps
today = date.today()
today = today.strftime("%Y-%m-%d")


# # host = "imap.gmail.com"
# download_folder = f"/home/viethoang/ai-academy/Gmail_preprocess/data/{today}"

# if not os.path.isdir(download_folder):
#     os.makedirs(download_folder, exist_ok=True)

#lấy file đính kèm trong mail gửi từ bên Minh Phú
class MailAgent:
    def __init__(
        self,
        email=EMAIL_INFO["email"],
        password=EMAIL_INFO["password"],
        host=EMAIL_INFO["host"],
    ):
        self.email = email
        self.password = password
        self.host = host

    def serverStart(self):
        mailbox = MailBox(EMAIL_INFO["host"])
        mailbox.login(
            EMAIL_INFO["email"], EMAIL_INFO["password"], initial_folder="INBOX"
        )
        return mailbox
#lấy mail ngày hôm nay
    def getUnseenMails(self, limit=100):
        today = date.today()

        mailbox = self.serverStart()
        # subjects = [msg for msg in mailbox.fetch(AND(all=True))]
        unseen_mails = mailbox.fetch(OR(date=today))
        return unseen_mails

    def getAllMails(self, limit=100):
        mailbox = self.serverStart()
        # subjects = [msg for msg in mailbox.fetch(AND(all=True))]
        all_mails = mailbox.fetch(AND(all=True))
        return all_mails

    def displayInfo(self, mode, limit=5):
        if mode.lower() == "unseen":
            mails = self.getUnseenMails()
        else:
            mails = self.getAllMails()
        for mail in mails:
            print("*********************")
            print(mail.from_)
            # print("Date: " + mail.date)
            # print("To: " + mail.to)
            # print("CC: ", mail.cc)
            # print("Title: " + mail.title)
            # print("Body: " + mail.body)
            print(type(mail.attachments))
#lấy file trong mail và lưu vào thư mục ứng với ngày gửi mail
    def getAttachment(self, mode):
        workingDir = os.path.abspath(os.curdir)
        count = 0
        if mode.lower() == "unseen":
            mails = self.getUnseenMails()
        else:
            mails = self.getAllMails()
        # print(len(mails))
        for mail in mails:
            count += 1
            mail_address = mail.from_
            mail_date = mail.date.date()

            # print(len(mail.attachments))
            download_folder = workingDir + f"/data/{mail_date}"
            print(download_folder)
            createDirectory(download_folder)
            for attachment in mail.attachments:
                print(attachment)
                filename, filecontent = (attachment.filename, attachment.payload)
                if ".XLSX" in filename:
                    try:
                        arr = filename.split(".XLSX")
                        filename = arr[0] + " (" + str(count) + ").XLSX"
                    except:
                        pass
                print(filename)
                if mail_address in MANAGER_MAIL_LIST:
                    folder_cate = "thanhpham"
                elif mail_address in SAlE_MAIL_LIST:
                    folder_cate = "donhang"
                elif mail_address in OPERATOR_MAIL_LIST:
                    folder_cate = "hopdong"
                else:
                    continue
                category_folder = f"{download_folder}/{folder_cate}"
                download_path = f"{download_folder}/{folder_cate}/{filename}"
                createDirectory(category_folder)
                print(download_path)
                print(filename)

                if ".XLSX" in filename or ".xlsx" in filename:
                    with open(download_path, "wb") as fp:
                        fp.write(filecontent)


def main_gmail_attachments():
    test = MailAgent()
    # test.displayInfo("unseen")
    test.getAttachment("unseen")


if __name__ == "__main__":
    main_gmail_attachments()
