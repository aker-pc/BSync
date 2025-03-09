from datetime import datetime
import json
import logging
import requests
import logging
import csv

logger = logging.getLogger(__name__)


class BNotion:

    def __init__(self, notion_config):
        self.notion_config = notion_config
        self.token = self.notion_config['token']
        self.parent_type = self.notion_config['type']
        self.database_id = self.notion_config['database_id']
        self.notion_body = self.load_body()


    def load_body(self):
        return {
            "parent": {
                "type": self.parent_type,
                "database_id": self.database_id,
            },
            "properties": {
                "日期": {
                    "date": {
                        "start": "",
                    }
                },
                "星期": {
                    "select": {
                        "name": "",  # 可选：周一、周二...周日
                    },
                },
                "账单信息": {
                    "title": [
                        {
                            "type": "text",
                            "text": {"content": ""},
                        }
                    ]
                },
                "金额": {
                    "number": 0.0,
                },
                "交易平台": {
                    "select": {
                        "name": "",  # 可选：支付宝、微信
                    },
                },
                "交易类型": {
                    "select": {
                        "name": "",  # 可选：收入、支出
                    },
                },
                "交易方式": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": ""},
                        }
                    ]
                },
                "平台交易单号": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": ""},
                        }
                    ]
                }
            }
        }

    def check_contrast(self, platform):
        # 第一次获取数据
        get_data = requests.request(
            'POST',
            'https://api.notion.com/v1/databases/' + self.database_id + '/query',
            headers={'Authorization': 'Bearer ' + self.token, 'Notion-Version': '2021-05-13'},
        )
        datas = json.loads(get_data.text)
        logger.debug(datas)
        temp_list = []
        temp_list.extend(datas['results'])

        # 如果一页不能加载完所有数据，继续获取，防止添加重复数据（折磨了作者三天才发现
        while datas['has_more']:
            logger.debug("数据量过大，正在获取更多数据")
            get_data = requests.request(
                'POST',
                'https://api.notion.com/v1/databases/' + self.database_id + '/query',
                headers={'Authorization': 'Bearer ' + self.token, 'Notion-Version': '2021-05-13'},
                json={'start_cursor': datas['next_cursor']}
            )
            datas = json.loads(get_data.text)
            logger.debug(datas)
            try:
                temp_list.extend(datas['results'])
                last_day = datetime.strptime(temp_list[-1]['properties']['日期']['date']['start'][:10], '%Y-%m-%d')
                now_day = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                if (now_day - last_day).days >= 100:
                    logger.info("已查询最近100天的数据")
                    break
            except KeyError as e:
                logger.info("Notion 数据库连接异常，请检查相关信息或集成")
                logger.debug(e)
                exit(-1)  # 终止程序

        bill_list = []
        for data in temp_list:
            # 根据平台筛选数据
            if data['properties']['交易平台']['select']['name'] == platform:
                try:
                    bill_list.append(data['properties']['平台交易单号']['rich_text'][0]['text']['content'])
                except IndexError:
                    logger.error("该记录未填写订单号 " + str(data))
        return bill_list

    def sync_bills(self, platform, update_data):
        nums = 0
        same_nums = 0
        bill_list = self.check_contrast(platform)

        if bill_list == -1:
            return -1

        week_list = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
        if platform == 'wechat':
            for bill in update_data:
                # 去除奇奇怪怪恼人的空格和占位符
                if bill[8].replace('\t', '').replace(' ', '') not in bill_list:
                    self.notion_body['properties']['日期']['date']['start'] = bill[0] + '+08:00'
                    self.notion_body['properties']['星期']['select']['name'] = week_list[
                        datetime.strptime(bill[0], "%Y-%m-%d %H:%M:%S").weekday()]
                    self.notion_body['properties']['金额']['number'] = float(bill[5].replace('¥', ''))
                    self.notion_body['properties']['交易平台']['select']['name'] = platform
                    self.notion_body['properties']['交易类型']['select']['name'] = bill[4]
                    self.notion_body['properties']['平台交易单号']['rich_text'][0]['text']['content'] = bill[8].replace(
                        '\t',
                        '').replace(' ',
                                    '')
                    if bill[4] == '支出':
                        self.notion_body['properties']['交易方式']['rich_text'][0]['text']['content'] = bill[6]
                        if bill[3] == '"/"':
                            self.notion_body['properties']['账单信息']['title'][0]['text']['content'] = bill[1]
                        else:
                            self.notion_body['properties']['账单信息']['title'][0]['text']['content'] = bill[3]
                    else:
                        self.notion_body['properties']['交易方式']['rich_text'][0]['text']['content'] = bill[7]
                        self.notion_body['properties']['账单信息']['title'][0]['text']['content'] = bill[1]

                    try:
                        send_data = requests.request(
                            'POST',
                            'https://api.notion.com/v1/pages',
                            json=self.notion_body,
                            headers={'Authorization': 'Bearer ' + self.token, 'Notion-Version': '2021-05-13'},
                        )
                        logger.debug(send_data.text)
                        nums += 1
                    except requests.exceptions.ConnectionError:
                        logger.error("订单号：" + bill[9] + "发送失败")
                        continue
                else:
                    same_nums += 1
        else:
            for bill in update_data:
                if bill[9].replace('\t', '').replace(' ', '') not in bill_list:
                    self.notion_body['properties']['日期']['date']['start'] = bill[0] + '+08:00'
                    self.notion_body['properties']['星期']['select']['name'] = week_list[
                        datetime.strptime(bill[0], "%Y-%m-%d %H:%M:%S").weekday()]
                    self.notion_body['properties']['账单信息']['title'][0]['text']['content'] = bill[4]
                    self.notion_body['properties']['金额']['number'] = float(bill[6].replace('¥', ''))
                    self.notion_body['properties']['交易平台']['select']['name'] = platform
                    self.notion_body['properties']['交易类型']['select']['name'] = bill[5]
                    self.notion_body['properties']['平台交易单号']['rich_text'][0]['text']['content'] = bill[9].replace(
                        '\t',
                        '').replace(' ',
                                    '')
                    if bill[5] == '支出':
                        self.notion_body['properties']['交易方式']['rich_text'][0]['text']['content'] = bill[7]
                    else:
                        self.notion_body['properties']['交易方式']['rich_text'][0]['text']['content'] = bill[8]

                    try:
                        send_data = requests.request(
                            'POST',
                            'https://api.notion.com/v1/pages',
                            json=self.notion_body,
                            headers={'Authorization': 'Bearer ' + self.token, 'Notion-Version': '2021-05-13'},
                        )
                        logger.info(send_data.text)
                        nums += 1
                    except requests.exceptions.ConnectionError:
                        logger.error("订单号：" + bill[9] + "发送失败")
                        continue

                else:
                    same_nums += 1
        logger.info("成功同步" + str(nums) + "条数据")
        logger.info("重复数据" + str(same_nums) + "条")

        return 0
