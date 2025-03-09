from sync_method import BNotion
from sync_method import BFeishu
import Bconfig
import Bmail
import BFile
import time
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
bconfig = Bconfig.Bconfig("/Users/a123/Documents/UGit/BSync/config/conf_demo.yaml")


bmail = Bmail.Bmail(bconfig.get_email_config())
# bnotion = BNotion.BNotion(bconfig.get_notion_config())
bfeishu = BFeishu.BFeishu(bconfig.get_feishu_config())

bmail.login()
for bill_platform, check_info in Bconfig.recv_config.items():
    mails = bmail.get_email(check_info)
    file_path = bmail.handle_email(mails, bill_platform)
    csv_data = BFile.BDataLoader.bill_data(file_path, bill_platform == "alipay")
    is_successes = bfeishu.sync_bills(bill_platform, csv_data)
    # is_successes = bnotion.sync_bills(bill_platform, notion_data)