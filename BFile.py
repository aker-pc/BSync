import os, csv
import zipfile
import shutil
import datetime
import logging


logger = logging.getLogger(__name__)

def run_once(func):
    func.has_run = False

    def wrapper(self, *args, **kwargs):
        if not func.has_run:
            result = func(self, *args, **kwargs)
            func.has_run = True
            return result
    return wrapper


class BDataLoader:

    @staticmethod
    def bill_data(filepath, is_alipay):
        data = []
        csv_encode = "gbk" if is_alipay else "utf-8"
        with open(filepath, "r", encoding=csv_encode) as f:
            reader = csv.reader(f)
            is_start = False
            jump_line = True
            logger.debug(reader)
            for row in reader:
                # 对商户名中出现英文逗号的情况进行处理
                while len(row) > 13:
                    row[2] += row[3]
                    row.pop(3)
                if not is_start:
                    if is_alipay:
                        # 支付宝可能存在空行，防止越界
                        for item in row:
                            if "电子客户回单" in item:
                                is_start = True
                                break
                    else:
                        # 与支付宝统一格式
                        for item in row:
                            if "微信支付账单明细列表" in item:
                                is_start = True
                                break
                    continue
                # 跳过说明行
                if jump_line:
                    jump_line = False
                    continue
                data.append(row)
        return data


class BFile:

    def __init__(self) -> None:
        self.save_path = os.getcwd()
        self.temp_path = self.save_path + '/bsync_save/temp'
        self.archives_path = self.save_path + '/bsync_save/archives'

    @run_once
    def init_path(self) -> None:
        if not os.path.exists(self.save_path + '/bsync_save'):
            logging.error(f"目标文件夹 {self.save_path} 不存在，创建该文件夹")
            os.mkdir(self.save_path + '/bsync_save')
        if not os.path.exists(self.save_path + '/bsync_save/temp'):
            logging.error(f"目标文件夹 {self.save_path} 不存在，创建该文件夹")
            os.mkdir(self.save_path + '/bsync_save/temp')

    @run_once
    def clear_his_bill(self):
        if not os.path.exists(self.temp_path):
            logger.error(f"源文件夹 {self.temp_path} 不存在")
            return

        for filename in os.listdir(self.temp_path):
            # 清空文件
            file_path = os.path.join(self.temp_path, filename)
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)
            else:
                os.remove(file_path)
        return


    def archive_his_bill(self, platform, unzip_path):
        if not os.path.exists(self.temp_path):
            logger.error(f"源文件夹 {self.temp_path} 不存在")
            return

        if platform != 'alipay' and platform != 'wechat':
            logger.error("平台名称错误")
            return

            # 创建目标文件夹
        if not os.path.exists(self.archives_path):
            logger.info(f"目标文件夹 {self.archives_path} 不存在，创建该文件夹")
            os.mkdir(self.archives_path)

        # 创建目标分类文件夹
        date_str = datetime.date.today().strftime("%Y%m%d")
        platform_path = os.path.join(self.archives_path, f"{platform}/{date_str}")
        if not os.path.exists(platform_path):
            logger.info(f"目标文件夹 {platform_path} 不存在，创建该文件夹")
            os.makedirs(platform_path)


        archive_file_path = os.path.join(platform_path, os.path.basename(unzip_path))
        shutil.move(unzip_path, archive_file_path)

    # 解压文件
    @staticmethod
    def unzip_with_password(zip_file, password, unzip_dir):
        logger.info(f"尝试使用密码 {password}")
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_file:
                for file in zip_file.namelist():
                    if file.endswith('.csv'):
                        zip_file.extract(member=file, pwd=password.encode('utf-8'), path=unzip_dir)

                # 首先检查目录下的直接文件
                for filename in os.listdir(unzip_dir):
                    if filename.endswith('.csv'):
                        return unzip_dir + '/' + filename

                # 然后遍历目录及其所有子目录
                for dir_path, dir_names, filenames in os.walk(unzip_dir):
                    for filename in filenames:
                        if filename.endswith('.csv'):
                            return dir_path + '/' + filename

        except FileNotFoundError:
            logger.error(f"文件 {zip_file} 不存在.")
            exit(-1)
        except zipfile.BadZipFile:
            logger.error(f"文件 {zip_file} 不是一个有效的zip文件.")
            exit(-1)
        except RuntimeError:
            logger.error(f"解压文件 {zip_file} 时密码错误.")
            exit(-1)
