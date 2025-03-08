import logging
import os
import random
import string
import time
import requests
import zmail
import re
from datetime import datetime, timedelta
import BFile

logger = logging.getLogger(__name__)


class Bmail:
    def __init__(self, email_config: dict) -> None:
        self.email_config = email_config
        self.zmail_server = None
        self.file_processor = BFile.BFile()


    def login(self):
        try:
            self.zmail_server = zmail.server(username=self.email_config['main']['address'],
                             password=self.email_config['main']['password'])
        except Exception as e:
            logger.error("登录服务端邮箱时出现异常 ")
            logger.error("请检查配置文件是否填写完整")
            logger.debug(e)
            return -1

        if self.zmail_server.smtp_able():
            logger.info("SMTP服务器连接成功")
            if self.zmail_server.pop_able():
                logger.error("POP3服务器连接成功")
                return 0
            else:
                logger.error("POP3服务器连接失败")
                return -1
        else:
            logger.error("SMTP服务器连接失败")
            return -1


    def get_email(self, conditions):
        try_times = 3
        while try_times > 0:
            try:
                # 补偿多轮次下接收支付宝和微信邮件的时间差
                mails_of_bills = self.zmail_server.get_mails(sender=conditions["sender"])
                return mails_of_bills
            except BrokenPipeError:
                logger.error("连接关闭，将尝试重新登录")
                self.login()
                continue
            except ConnectionResetError:
                logger.error("连接重置，即将重试")
                time.sleep(self.email_config["interval"])
                try_times -= 1
                continue
            except Exception as e:
                logger.debug("在获取支付宝邮件时异常 " + str(e))
                logger.error("收取邮件时遇到错误，即将重试")
                time.sleep(self.email_config["interval"])
                try_times -= 1
                continue
        logger.error("获取邮件重试次数过多，已停止获取")


    def handle_email(self, mail_info, bill_platform):
        is_alipay = bill_platform == "alipay"
        is_cmd = self.email_config["password_type"] == "command"
        unzip_path = ""
        if len(mail_info) != 0:
            self.file_processor.init_path()
            self.file_processor.clear_his_bill()
            single_mail = mail_info[0]
            if is_alipay:
                zmail.save_attachment(single_mail, self.file_processor.temp_path, overwrite=True)
                if is_cmd:
                    unzip_path = self.unzip_by_input(single_mail['attachments'][0][0])
                else:
                    unzip_path = self.unzip_by_email(single_mail, "支付宝", single_mail['attachments'][0][0])
            else:
                content = str(single_mail['content_html'])
                url = re.findall(r'https://download\.bill\.weixin\.qq\.com/[^"]*', content)
                download_path = self.file_processor.temp_path + '/wechat_bill' + datetime.now().strftime(
                    "%Y%m%d%H%M%S") + '.zip'
                response = requests.get(url[0])
                with open(download_path, 'wb') as fd:
                    for chunk in response.iter_content(chunk_size=1024):
                        fd.write(chunk)
                if is_cmd:
                    unzip_path =  self.unzip_by_input(os.path.basename(download_path))
                else:
                    unzip_path = self.unzip_by_email(single_mail, "微信", os.path.basename(download_path))
        if self.email_config["is_archive"]:
            self.file_processor.archive_his_bill(bill_platform, unzip_path)
        return unzip_path


    def unzip_by_email(self, single_mail, platform, zip_path):
        def generate_code(length):
            # 获取所有字母和数字的数组
            code_chars = string.ascii_letters + string.digits
            # 随机生成 length 个随机码
            return ''.join(random.choice(code_chars) for _ in range(length))

        zip_path = self.file_processor.temp_path + '/' + zip_path

        # 从邮箱获取解压密码
        random_code = generate_code(6)
        start_time = datetime.now()
        black_list = [single_mail['Id']]
        self.send_email(subject=f"<{random_code}>{platform}账单密码请求",
                   content=f'你正在对{platform}进行记账操作，你需要对这封邮件回复6位数字解压密码，有效时间2小时，发信时间为'
                           + datetime.now().strftime("%m-%d %H:%M:%S") + '。')

        # 轮询邮箱获取密码
        interval = self.email_config["main"]["interval"]
        time.sleep(interval)
        is_loop = True
        try_times = 3
        while is_loop and try_times > 0:
            # 时限为2小时
            if datetime.now() - start_time > timedelta(hours=2):
                break
            # 接收请求邮箱之后的回复邮箱
            try:
                mail_for_pwd = self.zmail_server.get_mails(subject=f'<{random_code}>{platform}账单密码请求',
                                                start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"))
            except ConnectionResetError:
                logger.error("连接重置，即将尝试重连")
                try_times -= 1
                time.sleep(interval)
                continue
            except BrokenPipeError:
                logger.error("连接关闭，将尝试重新登录")
                try_times -= 1
                self.login()
                continue
            except Exception as e:
                logger.debug(f"在获取{platform}文件密码时异常 " + str(e))
                logger.error("收取邮件时遇到错误，即将重试")
                try_times -= 1
                time.sleep(interval)
                continue

            if len(mail_for_pwd) != 0:
                for i in range(0, len(mail_for_pwd)):
                    # 跳过黑名单的邮件
                    if mail_for_pwd[i]['Id'] in black_list:
                        continue
                    # 尝试获取密码
                    zip_password = re.search(r'\d{6}', mail_for_pwd[i]['Content_text'][0]).group()
                    state = self.file_processor.unzip_with_password(zip_path, zip_password,
                                                self.file_processor.temp_path)
                    if state == 1:
                        is_loop = False
                        break
                    elif state == 2:
                        logger.info('文件失效，该链接可能已过期')
                        os.remove(zip_path)
                        break
                    elif state == 3:
                        black_list.append(mail_for_pwd[i]['id'])
                        self.send_email(subject=f'<{random_code}>{platform}账单密码请求',
                                   content=f'密码 {zip_password} 错误 \n你正在对{platform}进行记账操作，你需要对这封邮件回复6位数字解压密码，发信时间为'
                                           + datetime.now().strftime("%m-%d %H:%M:%S") + '。')
                    else:
                        os.remove(zip_path)
                        logger.info('解压成功')
                        logger.debug(f'解压成功，密码为{zip_password}')
                        if self.email_config['assist']['delete_after_used']:
                            logger.debug(mail_for_pwd[i]['Id'])
                            self.zmail_server.delete(single_mail['Id'])
                            logger.info('邮件已删除')
                        return state
            logger.info(f"未获取{platform}账单解压密码，即将重试")
            time.sleep(interval)
        if try_times == 0:
            return -4
        logger.info(f"2小时内未能获取{platform}账单解压密码")
        self.send_email(subject="同步失败", content="未能获取解压密码")
        return -3

    def send_email(self, subject, content):
        mail = {
            'subject': subject,
            'content_text': content
        }
        self.zmail_server.send_mail(self.email_config["assist"]["user_address"], mail)
        logger.info("请求邮件已发送")


    def unzip_by_input(self, file_path):
        unzip_password = input("按需输入密码：")
        state = self.file_processor.unzip_with_password(
            self.file_processor.temp_path + '/' + file_path, unzip_password,
            self.file_processor.temp_path)

        if type(state) != int:
            return state
        else:
            logger.error("unzip error")
            exit(0)