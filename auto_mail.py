from utils import EMAIL_INFO, MESSAGE_INFO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import mimetypes, requests
import pandas as pd
from datetime import datetime, timedelta
from utils import EMPLOYEE_INFO, createDirectory, findfiles
import glob
import json

#class tự động gửi mail
class MailSender:
    def __init__(self, email=EMAIL_INFO["email"], password=EMAIL_INFO["password"]):
        self.email = email
        self.password = password

    def messageConfig(self, receiver_address):
        message = MIMEMultipart()
        message["From"] = self.email
        message["To"] = receiver_address
        message["Subject"] = MESSAGE_INFO["subject"]
        # The subject line
        # The body and the attachments for the mail
        return message
    #đính kèm file khi gửi
    def attachFile(self, message, attach_file_names):
        # Setup the MIME
        # message = messageConfig()
        for attach_file_name in attach_file_names:
            attach_file = open(attach_file_name, "rb")  # Open the file as binary mode
            payload = MIMEBase("application", "octate-stream")
            payload.set_payload((attach_file).read())
            encoders.encode_base64(payload)  # encode the attachment
            # add payload header with filename
            ctype, encoding = mimetypes.guess_type(attach_file_name)
            if ctype is None or encoding is not None:
                ctype = "application/octet-stream"

            maintype, subtype = ctype.split("/", 1)

            if maintype == "text":
                fp = open(attach_file_name)
                # Note: we should handle calculating the charset
                attachment = MIMEText(fp.read(), _subtype=subtype)
                fp.close()
            elif maintype == "image":
                fp = open(attach_file_name, "rb")
                attachment = MIMEImage(fp.read(), _subtype=subtype)
                fp.close()
            elif maintype == "audio":
                fp = open(attach_file_name, "rb")
                attachment = MIMEAudio(fp.read(), _subtype=subtype)
                fp.close()
            else:
                fp = open(attach_file_name, "rb")
                attachment = MIMEBase(maintype, subtype)
                attachment.set_payload(fp.read())
                fp.close()
                encoders.encode_base64(attachment)
            attachment.add_header(
                "Content-Disposition",
                "attachment",
                filename=attach_file_name.replace(f"output/{today}/", ""),
            )
            message.attach(attachment)
        return message
    #gửi mail đến địa chỉ nào đó với config kèm với file đính kèm
    def sendEmail(self, config, filename, receiver_address):
        # filename = "/home/viethoang/ai-academy/Gmail_preprocess/test.xlsx"
        message = self.messageConfig(receiver_address)
        message.attach(MIMEText(MESSAGE_INFO["body"].format(config), "plain"))
        message = self.attachFile(message, filename)
        server = smtplib.SMTP(EMAIL_INFO["host"])
        server.starttls()
        server.login(EMAIL_INFO["email"], EMAIL_INFO["password"])
        server.sendmail(self.email, receiver_address, message.as_string())
        server.quit()

#lấy output từ các api nhắc lịch xuất ra file excel
def jsonToExcel(jsondata, filepath):
    # jsondata = requests.get(endpoint)
    print(type(jsondata.content))
    print(len(list(jsondata.content)))
    if len(jsondata.content) > 2:
        df = pd.read_json(jsondata.content)
        df.to_excel(filepath, index=False)


def addFile(endpoint, filepath):
    jsondata = requests.get(endpoint)
    jsonToExcel(jsondata, filepath)


def createDailyFolder():
    global today
    today = datetime.now()
    today = today.strftime("%Y-%m-%d")

    print(today)
    createDirectory(f"output/{today}")


def main_auto_mail():
    createDailyFolder()
    # neu so luong san xuat
    EMPLOYEE_INFO = requests.get("http://127.0.0.1:8000/api/saleinfo/")
    EMPLOYEE_INFO = json.loads(EMPLOYEE_INFO.content)
    print(EMPLOYEE_INFO)
    for employee in EMPLOYEE_INFO:
        print(employee)
        user_id = employee["id"]
        incoming_order_endpoint = (
            "http://127.0.0.1:8000/api/orders/?user_id={}&employee={}".format(
                user_id, employee["fullname"].upper()
            )
        )
        incoming_order_filepath = (
            "output/{today}/{employee}-danh sách đơn hàng sắp đến hạn.xlsx".format(
                today=today, employee=employee["fullname"]
            )
        )
        addFile(incoming_order_endpoint, incoming_order_filepath)

        outofstock_order_endpoint = (
            "http://127.0.0.1:8000/api/customer/?user_id={}&employee={}".format(
                user_id, employee["fullname"].upper()
            )
        )  # thay bằng endpoint khi đẩy lên server
        outofstock_order_filepath = (
            "output/{today}/{employee}-khach hang sap het don.xlsx".format(
                today=today, employee=employee["fullname"]
            )
        )
        addFile(outofstock_order_endpoint, outofstock_order_filepath)

        filenames = findfiles(
            "{employee}-*.xlsx".format(employee=employee["fullname"]),
            f"output/{today}",
        )
        print(filenames)
        if len(filenames) > 0:
            test = MailSender()
            test.sendEmail(
                employee["fullname"],
                filenames,
                "hoangso8000@gmail.com",
            )  # employee["email"]
            print("Mail Sended")

#gửi mail với nhắc lịch là lịch sử mua hàng
def history_auto_mail():
    createDailyFolder()
    EMPLOYEE_INFO = requests.get("http://127.0.0.1:8000/api/saleinfo/")
    EMPLOYEE_INFO = json.loads(EMPLOYEE_INFO.content)
    print(EMPLOYEE_INFO)
    for employee in EMPLOYEE_INFO:
        history_order_endpoint = (
            "http://127.0.0.1:8000/api/history/?employee={employee}".format(
                employee=employee["fullname"].upper()
            )
        )
        history_order_filepath = (
            "output/{today}/{employee}-lịch sử mua hàng.xlsx".format(
                today=today, employee=employee["fullname"].upper()
            )
        )
        addFile(history_order_endpoint, history_order_filepath)
        filenames = findfiles(
            "{employee}-lịch*.xlsx".format(employee=employee["fullname"]),
            f"output/{today}",
        )
        print(filenames)
        if len(filenames) > 0:
            test = MailSender()
            test.sendEmail(
                employee["fullname"],
                filenames,
                "hoangso8000@gmail.com",
            )  # employee["email"]
            print("Mail Sended")


if __name__ == "__main__":
    main_auto_mail()
    # history_auto_mail()
