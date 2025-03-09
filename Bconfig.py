import yaml

recv_config = {
    "alipay": {
        "sender" : "service@mail.alipay.com",
    },
    "wechat": {
        "sender" : "wechatpay@tencent.com",
    }
}


class Bconfig:

    def __init__(self, config_path):
        with open(config_path, encoding='utf-8') as yaml_file:
            self.__config = yaml.safe_load(yaml_file)

    def is_gui(self) -> bool:
        return self.__config['with_gui_start']


    def get_email_config(self) -> dict:
        return self.__config["email"]


    def get_notion_config(self) -> dict:
        return self.__config["notion"]


    def get_feishu_config(self) -> dict:
        return self.__config["feishu"]


    # TODO: 配置文件字段异常捕获
    def set_main_mail(self, server_address: str | None, server_password: str | None, interval: int | None):
        if server_address is not None:
            self.__config["email"]["main"]["server_address"] = server_address
        if server_password is not None:
            self.__config["email"]["main"]["server_password"] = server_password
        if interval is not None:
            self.__config["email"]["main"]["interval"] = interval


    def set_assist_mail(self, user_address: str | None, delete_after_used: bool | None):
        if user_address is not None:
            self.__config["email"]["assist"]["user_address"] = user_address
        if delete_after_used is not None:
            self.__config["email"]["assist"]["delete_after_used"] = delete_after_used


    def set_notion(self, database_id: str | None, token: str | None, notion_type: str | None):
        if database_id is not None:
            self.__config["notion"]["database_id"] = database_id
        if token is not None:
            self.__config["notion"]["token"] = token
        if notion_type is not None:
            self.__config["notion"]["type"] = notion_type