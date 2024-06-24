import os
from dotenv import find_dotenv, load_dotenv
from typing import List
from pydantic import BaseModel
import yagmail

# Load dotenv
load_dotenv(find_dotenv(".env"))

HOST = os.getenv("MAIL_HOST")
USERNAME = os.getenv("MAIL_USERNAME")
EMAIL = os.getenv("MAIL_EMAIL")
PASSWORD = " ".join(os.getenv("MAIL_PASSWORD").split("-"))
PORT = os.getenv("MAIL_PORT")


class MailBody(BaseModel):
    to: List[str]
    subject: str
    body: str


def send_mail(data: MailBody | None = None):
    yag = yagmail.SMTP(USERNAME, PASSWORD)

    try:
        yag.send(data['to'], data'subject'], data["body"])
        print("Mail send!")
        return {"status": 200, "errors": None}
    except Exception as e:
        print("Error sendinf email!")
        return {"status": 500, "errors": e}
