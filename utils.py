from datetime import date
import glob
import fnmatch
import string
from pyvi import ViUtils

EMAIL_INFO = {
    "host": "",
    "email": "",
    "password": "",
}
# DATABASE_INFO = {
#     "host": "",
#     "database": "",
#     "user": "",
#     "password": "",
#     "port": ,
# }
DATABASE_INFO = {
    "host": "",
    "database": "",
    "user": "",
    "password": "",
    "port": 5432,
}
SAlE_MAIL_LIST = ["quangson@minhphu.com", "vanlen@minhphu.com"]
MANAGER_MAIL_LIST = [
    "ducchuc@minhphu.com",
    "thuynga@minhphu.com",
    "viettrung@minhphu.com",
    "hanguyen@minhphu.com",
]
OPERATOR_MAIL_LIST = ["xuannhi@minhphu.com", "hoangso8000@gmail.com"]
MESSAGE_INFO = {
    "subject": "Nhắc lịch đơn hàng",
    "body": "Chào bạn {},\nGửi bạn nhắc lịch được đính kèm trong các file dưới đây\nThân chào!",
}
EMPLOYEE_INFO = {
    "VŨ VĂN KHƯƠNG": "vukhuong@minhphu.com",
    "ĐẶNG QUANG SƠN": "quangson@minhphu.com",
    "NGÔ THỊ BỬU HƯỜNG": "huongngo@minhphu.com",
    "TRẦN ĐĂNG KHOA": "khoatran@minhphu.com",
    "TRẦN VĂN LEN": "vanlen@minhphu.com",
}
import re
import base64, quopri
import os


def encoded_words_to_text(encoded_words):
    encoded_word_regex = r"=\?{1}(.+)\?{1}([B|Q])\?{1}(.+)\?{1}="
    charset, encoding, encoded_text = re.match(
        encoded_word_regex, encoded_words
    ).groups()
    if encoding is "B":
        byte_string = base64.b64decode(encoded_text)
    elif encoding is "Q":
        byte_string = quopri.decodestring(encoded_text)
    return byte_string.decode(charset)


def createDirectory(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)

# tìm file theo regex
def findfiles(which, where="."):
    """Returns list of filenames from `where` path matched by 'which'
    shell pattern. Matching is case-insensitive."""

    # TODO: recursive param with walk() filtering
    rule = re.compile(fnmatch.translate(which), re.IGNORECASE)
    return [where + "/" + name for name in os.listdir(where) if rule.match(name)]


def no_accent_vietnamese(s):
    s = re.sub(r"[àáạảãâầấậẩẫăằắặẳẵ]", "a", s)
    s = re.sub(r"[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]", "A", s)
    s = re.sub(r"[èéẹẻẽêềếệểễ]", "e", s)
    s = re.sub(r"[ÈÉẸẺẼÊỀẾỆỂỄ]", "E", s)
    s = re.sub(r"[òóọỏõôồốộổỗơờớợởỡ]", "o", s)
    s = re.sub(r"[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]", "O", s)
    s = re.sub(r"[ìíịỉĩ]", "i", s)
    s = re.sub(r"[ÌÍỊỈĨ]", "I", s)
    s = re.sub(r"[ùúụủũưừứựửữ]", "u", s)
    s = re.sub(r"[ƯỪỨỰỬỮÙÚỤỦŨ]", "U", s)
    s = re.sub(r"[ỳýỵỷỹ]", "y", s)
    s = re.sub(r"[ỲÝỴỶỸ]", "Y", s)
    s = re.sub(r"[Đ]", "D", s)
    s = re.sub(r"[đ]", "d", s)
    return s


def processColumn(df):
    df.columns = [
        no_accent_vietnamese(
            column.strip()
            .replace("(", "")
            .replace(")", "")
            .replace(" ", "_")
            .replace("/", "")
            .replace(".", "")
            .replace("__", "_")
        )
        for column in df.columns
