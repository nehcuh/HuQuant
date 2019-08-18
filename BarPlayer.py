"""
行情播放器，本质而言，while 循环，不断去数据接口请求数据
可以根据需求，自行更改数据接口，但是数据格式需要统一
"""

import datetime
import threading
import time
from queue import Queue

import jqdatasdk
import pandas as pd

from Strategy import Strategy

jqdatasdk.auth("JQDATAUSER", "JQDATAPASSWD")


class BarPlayer:
    """
    行情播放器，目前仅介入聚宽的分钟行情源
    """

    def __init__(self):
        self.cursor_date = str(datetime.date.today())[:10]
        # 聚宽行情，默认当前分钟 10 秒之后可以获取前一分钟行情
        self.day_start = pd.Timestamp(self.cursor_date + " 09:32:00")
        # 行情结束时间
        self.day_end = pd.Timestamp(self.cursor_date + " 15:00:00")
        # 当前时间标尺
        self.cursor_time = datetime.datetime.now()
        # 是否交易标志
        self.on_trade_flag = True
        # 股票信息
        df_stock_info = (
            jqdatasdk.get_all_securities(types="stock")
            .reset_index()
            .rename(columns={"index": "stock_code", "display_name": "stock_name"})
        )

    def bind_strategy(self, strategy):
        """
        与策略绑定
        """
        # 1. 策略绑定
        self.strategy = strategy
        # 2. 当前交易日映射到周线日期
        # 2.1 如果当前交易日在周线日期之前，直接沿用周线日期
        # 2.2 如果当前交易日在周线日期之后，更新周线日期为该交易日所在周的周末日期
        if (
            pd.Timestamp(self.cursor_date)
            < self.strategy.weekly_stock_dict["stock_close"].index[-1]
        ):
            self.current_weekly_trade_date = str(
                self.strategy.weekly_stock_dict["stock_code"].index[-1]
            )[:10]
        else:
            self.current_weekly_trade_date = pd.Timestamp(
                self.cursor_date
            ) + pd.Timedelta(days=(7 - pd.Timestamp(self.cursor_date).isoweekday()))
        # 3. 订阅股票池，包括： 1. 提前选好的股票池 2. 持仓股票
        # 同时将股票信息传递给策略类
        stock_list = self.strategy.subscribe_stocks
        self.strategy.stock_info_dict = self.stock_info_dict
        self.stock_list = list(
            map(lambda x: x + ".XSHG" if x[0] == "6" else x + ".XSHE", stock_list)
        )

    def if_tradetime(self, datetime: str):
        """
        交易时间判断: 可以在非交易时段不去获取数据
        :param datetime: 时间
        """
        if datetime < self.day_start:
            return False
        elif datetime > pd.Timestamp(
            self.cursor_date + " 11:31:00"
        ) and datetime < pd.Timestamp(self.cursor_date + " 13:02:00"):
            return False
        else:
            return True

    def get_min_data(
        self, stock_list: list, start_time: str, end_time: str, type_: str = "stock"
    ):
        """
        获取分钟数据
        :param stock_list: 股票池
        :param start_time: 起始时间
        :param end_time: 结束时间
        :param type_: 股票 "stock" 或者 指数 "index" 类型选择
        :return: panel 数据格式 (聚宽取数据如果是多标的，返回的是 panel 格式)
        e.g.
        Dimensions: 6 (items) x 2 (major_axis) x 2 (minor_axis)
        Items axis: close to volume
        Major_axis axis: 2019-01-21 10:34:00 to 2019-01-21 10:35:00
        Minor_axis axis: 600000.XSHG to 000001.XSHE
        """
        if type_ not in ["stock", "index"]:
            raise ValueError("错误，目前仅支持股票与指数行情获取")
        if type_ == "stock":
            return jqdatasdk.get_price(
                security=stock_list,
                start_date=start_time,
                end_date=end_time,
                frequency="1m",
                fq="None",
            )
        else:
            return jqdatasdk.get_price(
                security="00000.XSHG",
                start_time=start_time,
                end_time=end_time,
                frequency="1m",
            )

    def bar_play(self):
        """
        行情播放器
        """
        current_time = datetime.datetime.now()
        # 分钟标尺，分钟切换判断需要
        cursor_min = current_time.minute
        while self.on_trade_flag:
            # 1. 不断读取分钟数据
            current_time = datetime.datetime.now()
            # 1.1 交易时段判断
            # 由于聚宽行情源的限制，仅能在当前分钟 bar 过 10 秒后才能获取前一分钟 bar
            if current_time > self.day_end:
                self.on_trade_flag = False
            elif (not self.if_tradetime(current_time)) or (current_time.second) < 10:
                time.sleep(1.0)
                continue
            # 当分钟切换时，可以获取前一分钟的行情
            curr_min_time = (
                str(current_time - datetime.timedelta(minutes=1))[:16] + ":00"
            )
            self.bar_time = curr_min_time[11:16]
            if current_time.minute != cursor_min:
                fetch_min_flag = True
                while fetch_min_flag:
                    try:
                        # 聚宽行情，默认获取数据为 frame
                        min_stock_data = self.get_min_data(
                            self.stock_list,
                            start_time=curr_min_time,
                            end_time=curr_min_time,
                            type_="stock",
                        ).to_frame()
                        min_stock_data = min_stock_data.reset_index().rename(
                            columns={"major": "datetime", "minor": "symbol"}
                        )
                        min_stock_data.symbol = min_stock_data.rename(
                            columns={"symbol": "stock_code"}
                        )
                        min_index_data = (
                            self.get_min_data(
                                start_time=curr_min_time,
                                end_time=curr_min_time,
                                type_="index",
                            )
                            .reset_index()
                            .rename(columns={"index": "datetime"})
                        )
                        min_index_data["stock_code"] = "000001"
                        # 数据保存，方便盘后检查
                        min_stock_data.to_csv(
                            "{}/stock_{}.csv".format(
                                self.cursor_date, curr_min_time[11:16].replace(":", "")
                            ),
                            index=False,
                        )
                        min_index_data.to_csv(
                            "{}/index_{}.csv".format(
                                self.cursor_date, curr_min_time[11:16].replace(":", "")
                            ),
                            index=False,
                        )
                    except:
                        if current_time > self.day_end:
                            fetch_min_flag = False
                        time.sleep(1.0)
                        # 聚宽的行情源不太稳定，如果经常行情中断，可以尝试重新登录
                        # jqdatasdk.auth("jqdatauser", "jqdatapasswd")
                        continue
                    if len(min_stock_data) != 0 or len(min_index_data) != 0:
                        fetch_min_flag = False
                        cursor_min = current_time.minute
                        # 做数据透视，方便向量化处理
                        min_stock_high = min_stock_data.pivot(
                            index="datetime", columns="stock_code", values="high"
                        )
                        min_stock_low = min_stock_data.pivot(
                            index="datetime", columns="stock_code", values="low"
                        )
                        min_stock_close = min_stock_data.pivot(
                            index="datetime", columns="stock_code", values="close"
                        )
                        min_stock_vol = min_stock_data.pivot(
                            index="datetime", columns="stock_code", values="volume"
                        )
                        min_stock_money = min_stock_data.pivot(
                            index="datetime", columns="stock_code", values="money"
                        )
                        min_stock_vmp = min_stock_money / min_stock_vol
                        min_index_high = min_index_data.pivot(
                            index="datetime", columns="stock_code", values="high"
                        )
                        min_index_low = min_index_data.pivot(
                            index="datetime", columns="stock_code", values="low"
                        )
                        min_index_close = min_index_data.pivot(
                            index="datetime", columns="stock_code", values="close"
                        )
                        min_bar_dict = {
                            "bar_time": self.bar_time,
                            "stock_high": min_stock_high.iloc[-1].rename(
                                self.current_weekly_trade_date
                            ),
                            "stock_low": min_stock_low.iloc[-1].rename(
                                self.current_weekly_trade_date
                            ),
                            "stock_close": min_stock_close.iloc[-1].rename(
                                self.current_weekly_trade_date
                            ),
                            "stock_vmp": min_stock_vmp.iloc[-1].rename(
                                self.current_weekly_trade_date
                            ),
                            "stock_vol": min_stock_vol.iloc[-1].rename(
                                self.current_weekly_trade_date
                            ),
                            "index_high": min_index_high.iloc[-1].rename(
                                self.current_weekly_trade_date
                            ),
                            "index_low": min_index_low.iloc[-1].rename(
                                self.current_weekly_trade_date
                            ),
                            "index_close": min_index_close.iloc[-1].rename(
                                self.current_weekly_trade_date
                            ),
                        }
                        print(min_stock_data)
                        # 将封装好的分钟行情传递给策略类
                        # self.strategy.on_bar(min_bar_dict)


if __name__ == "__main__":
    bar_player = BarPlayer()
    bar_player.bar_play()
