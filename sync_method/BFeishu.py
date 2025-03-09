from datetime import datetime
import json
import logging
import requests
import logging
import csv

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *

logger = logging.getLogger(__name__)


class BFeishu:

    def __init__(self, feishu_config):
        self.feishu_config = feishu_config
        self.app_id = self.feishu_config['app_id']
        self.app_secret = self.feishu_config['app_secret']
        self.app_token = self.feishu_config['app_token']
        self.table_id = self.feishu_config['table_id']
        self.feishu_clent = self.feishu_client_start()
        self.feishu_body = self.load_body()


    def load_body(self):
        return {
            "平台交易单号": "",
            "日期": 0,
            "星期": "",
            "账单信息": "",
            "金额": 0,
            "交易平台": "",
            "交易类型": "",
            "交易方式": ""
        }


    def feishu_client_start(self):
        return lark.Client.builder() \
        .app_id(self.app_id) \
        .app_secret(self.app_secret) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()


    def sync_bills(self, platform, update_data):
        def convert_time(time_str) -> int:
            format_str = '%Y-%m-%d %H:%M:%S'
            dt_obj = datetime.strptime(time_str, format_str)
            timestamp_seconds = dt_obj.timestamp()
            timestamp_milliseconds = int(timestamp_seconds * 1000)
            return timestamp_milliseconds


        same_nums = 0
        bill_list = []

        record_list = []

        week_list = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
        if platform == 'wechat':
            for bill in update_data:
                # 去除奇奇怪怪恼人的空格和占位符
                if bill[8].replace('\t', '').replace(' ', '') not in bill_list:
                    self.feishu_body['日期'] = convert_time(bill[0])
                    self.feishu_body['星期'] = week_list[
                        datetime.strptime(bill[0], "%Y-%m-%d %H:%M:%S").weekday()]
                    self.feishu_body['金额'] = float(bill[5].replace('¥', ''))
                    self.feishu_body['交易平台'] = platform
                    self.feishu_body['交易类型'] = bill[4]
                    self.feishu_body['平台交易单号'] = bill[8].replace(
                        '\t',
                        '').replace(' ',
                                    '')
                    if bill[4] == '支出':
                        self.feishu_body['交易方式'] = bill[6]
                        if bill[3] == '"/"':
                            self.feishu_body['账单信息'] = bill[1]
                        else:
                            self.feishu_body['账单信息'] = bill[3]
                    else:
                        self.feishu_body['交易方式'] = bill[7]
                        self.feishu_body['账单信息'] = bill[1]

                    record_list.append(AppTableRecord.builder().fields(self.feishu_body.copy()).build())

                else:
                    same_nums += 1
        else:
            for bill in update_data:
                if bill[9].replace('\t', '').replace(' ', '') not in bill_list:
                    self.feishu_body['日期'] = convert_time(bill[0])
                    self.feishu_body['星期'] = week_list[
                        datetime.strptime(bill[0], "%Y-%m-%d %H:%M:%S").weekday()]
                    self.feishu_body['账单信息'] = bill[4]
                    self.feishu_body['金额'] = float(bill[6].replace('¥', ''))
                    self.feishu_body['交易平台'] = platform
                    self.feishu_body['交易类型'] = bill[5]
                    self.feishu_body['平台交易单号'] = bill[9].replace(
                        '\t',
                        '').replace(' ',
                                    '')
                    if bill[5] == '支出':
                        self.feishu_body['交易方式'] = bill[7]
                    else:
                        self.feishu_body['交易方式'] = bill[8]

                    record_list.append(AppTableRecord.builder().fields(self.feishu_body.copy()).build())
                else:
                    same_nums += 1

        request: BatchCreateAppTableRecordRequest = BatchCreateAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(self.table_id) \
            .request_body(BatchCreateAppTableRecordRequestBody.builder()
                          .records(record_list)
                          .build()) \
            .build()

        # 发起请求
        response: BatchCreateAppTableRecordResponse = self.feishu_clent.bitable.v1.app_table_record.batch_create(request)

        # 处理失败返回
        if not response.success():
            logger.error(
                f"client.bitable.v1.app_table_record.batch_create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
            return

        # 处理业务结果
        logger.info(lark.JSON.marshal(response.data, indent=4))

        return 0
