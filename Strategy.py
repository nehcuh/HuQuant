"""
这里用简单的均线策略作为示例
"""
import datetime
import os
import sys
import time
from queue import Queue

import jqdatasdk
import numpy as np
import pandas as pd

from trade_dates import util_get_real_trade_date

jqdatasdk.auth("JQUSERNAME", "JQDATAPASSWD")


class Strategy:
    """
    策略类：
    1. 对通过 BarPlayer 接入的行情进行处理
    2. 维护本地账户
    3. 模拟买入卖出
    """

    def __init__(self):
        """
      初始化工作
      """
        # 今日交易股票
        self.on_trade_stocks = set()
        # 订单簿
        self.order_book = self.init_order_book()
        # 信号触发计数
        self.signal_trigger_dict = {}  # 避免假信号，信号 N 次后买入
        # 账户信息
        self.account_position = pd.DataFrame()
        # 当前交易日
        self.current_trade_date = None
        # 周线数据
        self.weekly_stock_dict = dict()
        # TODO: 开盘需要卖出的股票
        # self.sell_on_open = None
        # 订阅股票
        self.subscribe_stocks = None
        # 账户资金
        self.account_cash = int(INIT_CASH)
        # 分钟 bar 时间
        self.bar_time = None
        # 分钟 bar 行情
        # self.min_bar_dict = dict()
        # 技术指标
        self.A = None
        self.B = None
        self.C = None
        self.D = None
        self.order_queue = Queue()  # 存放 Order 到队列中
        self.stock_info_dict = dict()  # 股票行情

    def init_order_book(self):
        return pd.DataFrame(
            columns=[
                "stock_code",  # 股票代码
                "trade_date",  # 交易日期
                "bar_time",  # bar 时间
                "order_direction",  # 买卖方向
                "order_amount",  # 订单委托数量
                "order_price",  # 订单委托价格
                "trade_amount",  # 成交数量
                "trade_price",  # 成交金额
                "trade_money",  # 交易金额 (成交金额，滑点等)
                "order_life_minutes",  # 订单持续时间 (拆单操作)
                "position_dates",  # 持有天数
            ]
        )

    def on_market_open(self):
        """
        开盘前需要处理的事务
        """
        # 当前交易日期
        self.current_trade_date = str(datetime.date.today())[:10]
        # 创建文件夹，存放相应交易日数据等
        if not os.path.exists("manual_order/{}}".format(self.current_trade_date)):
            os.mkdir("manual_order/{}".format(self.current_trade_date))
        # 账户信息
        previous_date = str(
            pd.Timestamp(self.current_trade_date) - pd.Timedelta(days=1)
        )[:10]
        # 获取上一个交易日期
        previous_trade_date = util_get_real_trade_date(previous_date)
        # 初始化账户信息
        self.init_account(previous_trade_date)
        # 订阅股票池
        self.subscribe_stocks = self.init_stocks()
        # 准备前置数据
        self.weekly_stock_dict = self.init_data(self.current_trade_date)

    def on_bar(self, min_bar_dict):
        """
        盘中分钟 bar 推送
        """
        self.update_weekly_bar(min_bar_dict)
        print("周线数据更新结束")
        # 3. TEST: 更新技术指标
        self.update_factors()
        print("技术指标更新结束")
        # 4. TEST: 更新交易信号
        stock_list = self.update_signals(min_bar_dict["bar_time"])
        print("交易信号更新结束")
        # 5. WAITING: 以非阻塞方式创建订单 => 另开一个进程，读取订单信号
        # 6. 创建订单
        if stock_list:
            filter_stock_list = sorted(list(set(stock_list) - self.on_trade_stocks))
            for stock in filter_stock_list:
                self.display.queue.put(
                    {"bar_time": min_bar_dict["bar_time"], "stock_code": stock, "stock_name": self.stock_info_dict[stock]})
                self.display.select_queue.put(
                    {"bar_time": min_bar_dict["bar_time"], "stock_code": stock, "stock_name": self.stock_info_dict[stock]})
            # print(stock_list)
            self.create_buy_orders(min_bar_dict)
            while not self.drop_queue.empty():
                print("丢弃股票")
                self.on_trade_stocks.add(self.drop_queue.get()["stock_code"])

        # 2. TEST: 交易模块
        self.on_trade(min_bar_dict)
        # 7. 更新持仓盈亏
        self.update_position_pnl(min_bar_dict)

        # 8. 盘中止损处理
        self.create_sell_order(min_bar_dict)

    def on_market_close(self):
        """
        收盘后需要处理的事务
        """
        print("盘后结算")
        self.order_book.to_csv(
            "{}/weekly_order_book.csv".format(self.current_trade_date),
            encoding="gbk")
        self.account_position["position_dates"] += 1
        self.account_position = self.account_position.drop_duplicates(subset=["stock_code", "position_dates"],
                                                                      keep="first")
        self.account_position.to_csv(
            "{}/weekly_account_position.csv".format(self.current_trade_date),
            encoding="gbk",
            index=False
        )
        with open("weekly_account_cash.txt", "a") as f:
            f.write(str(self.account_cash)+"\n")

    def load_sell_stocks(self, trade_date):
        """
        加载今日开盘需要卖出的股票
        将需要卖出的股票保存在当前交易日命名的文件夹下，按照每行一个股票代码的形式保存
        """
        try:
            return sorted(
                pd.read_csv("{}/sell_stock.txt".format(trade_date), header=None)[0]
                .map(str)
                .str.zfill(6)
                .tolist()
            )
        except FileNotFoundError:
            return []

    def init_stocks(self):
        """
        订阅股票池
        """
        try:
            return sorted(
                list(
                    set(
                        pd.read_csv(
                            "{}/weekly_stock_pool.txt".format(
                                str(datetime.date.today())
                            )
                        )
                    )
                )
            )
        except FileNotFoundError:
            return []

    def init_data(self, trade_date):
        """
        数据初始化
        """
        # 指数日线数据
        index_daily_info = jqdatasdk.get_price(
            security="000001.XSHG",
            start_date="2017-01-01",
            end_date=self.current_trade_date,
        )
        index_daily_info.index = pd.to_datetime(index_daily_info.index)
        index_daily_high = index_daily_info.[["high"]].rename(columns={"close": "000001"})
        index_daily_low = index_daily_info.[["low"]].rename(columns={"close": "000001"})
        index_daily_close = index_daily_info.[["close"]].rename(columns={"close": "000001"})
        # 股票日线数据
        stock_daily_info = jqdatasdk.get_price(
            security=self.subscribe_stocks,
            start_date="2017-01-01",
            end_date=self.current_trade_date,
        )
        df_stock_daily = stock_daily_info.to_frame().reset_index().rename(columns={"minor": "code", "major": "date"})
        df_stock_daily['code'] = df_stock_daily["code"].map(str).str.slice(0, 6)
        stock_daily_high = df_stock_daily.reset_index().pivot(index="date", columns="code", values="high")
        stock_daily_low = df_stock_daily.reset_index().pivot(index="date", columns="code", values="low")
        stock_daily_close = df_stock_daily.reset_index().pivot(index="date", columns="code", values="close")

        stock_daily_high.index = pd.to_datetime(stock_daily_high.index)
        stock_daily_low.index = pd.to_datetime(stock_daily_low.index)
        stock_daily_close.index = pd.to_datetime(stock_daily_close.index)

        # 周线数据处理
        index_weekly_high = index_daily_high.resample("w", closed="right").apply("max")
        index_weekly_low = index_daily_high.resample("w", closed="right").apply("min")
        index_weekly_close = index_daily_high.resample("w", closed="right").apply("last")
        stock_weekly_high = stock_daily_high.resample("w", closed="right").apply("max")
        stock_weekly_low = stock_daily_high.resample("w", closed="right").apply("min")
        stock_weekly_close = stock_daily_high.resample("w", closed="right").apply("last")

        # 考虑到春节等长假影响，可能会有一周以上的缺口
        index_weekly_high = index_weekly_high.dropna()
        index_weekly_low = index_weekly_low.dropna()
        index_weekly_close = index_weekly_close.dropna()


        stock_weekly_high = stock_weekly_high.loc[index_weekly_high.index]
        stock_weekly_low = stock_weekly_low.loc[index_weekly_low.index]
        stock_weekly_close = stock_weekly_close.loc[index_weekly_low.index]

        return {
            "stock_high": stock_weekly_high,
            "stock_low": stock_weekly_low,
            "stock_close": stock_weekly_close,
            "index_high": index_weekly_high,
            "index_low": index_weekly_low,
            "index_close": index_weekly_close,
        }

    def init_account(self, trade_date):
        """
        初始化账户信息
        """
        try:
            self.account_position = pd.read_csv(
                "{}/weekly_account_position.csv".format(trade_date)
            )
            self.account_position.stock_code = self.account_position.stock_code.map(
                str
            ).str.zfill(6)
            # 已经有持仓，不再买入
            self.on_trade_stocks = set(self.account_position.stock_code.tolist())
        except FileNotFoundError:
            self.account_position = pd.DataFrame(
                columns=[
                    "stock_code",  # 股票代码
                    "trade_date",  # 交易日期
                    "bar_time",  # bar 时间
                    "position_amount",  # 持仓数量
                    "buy_price",  # 买入价格
                    "buy_amount",  # 买入数量
                    "cost_price",  # 实际价格
                    "buy_adjust_factor",  # 买入时复权因子
                    "last_price",  # 最新价
                    "pnl_rate",  # 浮盈
                    "position_dates",  # 持仓天数
                ]
            )

    def on_trade(self, min_bar_dict):
        """
        模拟成交
        """
        # 1. 更新本分钟 bar 的成交均价与成交量
        del self.order_book["trade_price"]
        self.order_book = pd.merge(
            left=self.order_book,
            right=pd.DataFrame(np.round(min_bar_dict["stock_vmp"].rename(
                "trade_price"), 2)).reset_index(),
            on="stock_code",
        )
        self.order_book = pd.merge(
            left=self.order_book,
            right=pd.DataFrame(min_bar_dict["stock_vol"].rename(
                "stock_vol")).reset_index(),
            on="stock_code",
        )
        # 2. 计算本分钟成交股数，按照 order_life_minutes 等比例成交
        self.order_book.loc[self.order_book["order_life_minutes"] == 0, "trade_amount"] = 0. # 避免出现除以 0 的错误
        self.order_book.loc[self.order_book["order_life_minutes"] != 0, "trade_amount"] = np.round(
            (self.order_book.loc[self.order_book["order_life_minutes"] != 0, "order_amount"] /
            self.order_book.loc[self.order_book["order_life_minutes"] != 0, "order_life_minutes"] / 100.0).astype("float")
        ) * 100
        # 3. 如果本分钟 bar 的成交量不足实际要成交的股数两倍，或本分钟成交均价有异常，本分钟不成交
        self.order_book.loc[
            (self.order_book["stock_vol"] < self.order_book["trade_amount"] *
             VOLUME_OFFSET)
            |
            (self.order_book["trade_price"].isnull()), "trade_amount", ] = 0.0
        # 4. 更新订单成交金额
        self.order_book["trade_money"] = (
            self.order_book["trade_amount"] * self.order_book["trade_price"])
        # 5. 删除订单簿中不需要的分钟 bar 成交量信息
        del self.order_book["stock_vol"]
        # 6. 保留订单流水
        # self.order_book["bar_time"] = min_bar_dict["bar_time"]
        # 7. 更新现金，卖出时考虑手续费与印花税
        self.account_cash += (
            -1 * self.order_book.loc[self.order_book["order_direction"] == 1].
            order_direction * self.order_book.loc[
                self.order_book["order_direction"] == 1].trade_money).sum()
        self.account_cash += (
            -1 * self.order_book.loc[self.order_book["order_direction"] ==
                                     -1].order_direction *
            self.order_book.loc[self.order_book["order_direction"] ==
                                -1].trade_money).sum() * (1 - COMMISSION_FEE)
        # 8. 订单成交，更新持仓信息
        trade_position = self.order_book[[
            "stock_code",
            "order_direction",
            "trade_amount",
            "trade_money",
            "position_dates",
        ]]
        # 为了避免出现浮点数为 1E-26 这样的问题，设置可成交订单交易数量必须大于 1
        trade_position = trade_position.loc[trade_position["trade_amount"] > 1]
        trade_position["trade_amount"] = (
            trade_position["trade_amount"] * trade_position["order_direction"])
        trade_position["trade_money"] = (
            trade_position["trade_money"] * trade_position["order_direction"])

        # 新建新持仓
        new_position = trade_position[["stock_code"]]
        new_position["trade_date"] = self.current_trade_date
        new_position["position_dates"] = 0
        new_position["position_amount"] = 0
        self.account_position = self.account_position.append(new_position, ignore_index=True).fillna(0)
        # 账户更新
        for index in trade_position.index:
            stock_code = trade_position.loc[index, "stock_code"]
            trade_amount = trade_position.loc[index, "trade_amount"]
            trade_money = trade_position.loc[index, "trade_money"]
            position_dates = trade_position.loc[index, "position_dates"]
            position_index = self.account_position.loc[
                    (self.account_position["stock_code"] == stock_code)
                    & (self.account_position["position_dates"] == position_dates
                    )].index[0]
            cost_price = (trade_money + (
                    self.account_position.loc[position_index, "position_amount"] *
                    self.account_position.loc[position_index, "cost_price"]).sum()
                          ) / (trade_amount +
                               (self.account_position.
                                loc[position_index, "position_amount"]).sum())
            # 订单成交，相应地，更新账户中持仓股数与持仓成本
            self.account_position.loc[position_index,
                                      "position_amount"] += trade_amount
            self.account_position.loc[position_index,
                                      "cost_price"] = cost_price
        # TEST: 记录订单信息
        # print(self.order_book)
        # print("订单信息: ", self.current_trade_date, self.bar_time, self.order_book.loc[self.order_book["trade_amount"] > 0])
        if self.order_book.loc[self.order_book["trade_amount"] > 0].shape[0] != 0:
            self.order_book.loc[self.order_book["trade_amount"] > 0].to_csv("Z:\\real_trade\\N2C\\manual_order\\{}\\{}_orders.csv".format(
                self.current_trade_date, min_bar_dict["bar_time"].replace(":", "")), index=False)

        # 9. 订单成交后，需要更新订单簿
        origin_order_life_minutes = self.order_book.loc[self.order_book["trade_amount"] > 0, "order_life_minutes"]
        self.order_book.loc[self.order_book["trade_amount"] > 0,
                            "order_life_minutes"] -= 1

        self.order_book.loc[self.order_book["trade_amount"] > 0,
                            "order_amount"] = self.order_book.loc[
                                                          self.order_book["trade_amount"] > 0, "order_amount"] * (
            self.order_book.loc[self.order_book["trade_amount"] > 0, "order_life_minutes"] / origin_order_life_minutes)
        # 10. 重置需要成交订单信息
        self.order_book["trade_amount"] = 0
        self.order_book["trade_price"] = 0
        self.order_book["trade_money"] = 0

    def update_weekly_bar(self, min_bar_dict):
        """
        更新周线数据
        """
        # 当周首个分钟 bar 数据
        if self.weekly_stock_dict["stock_high"].index[-1] < pd.Timestamp(
                self.current_trade_date):
            self.weekly_stock_dict["stock_high"] = self.weekly_stock_dict[
                "stock_high"].append(min_bar_dict["stock_high"])
            self.weekly_stock_dict["stock_low"] = self.weekly_stock_dict[
                "stock_low"].append(min_bar_dict["stock_low"])
            self.weekly_stock_dict["stock_close"] = self.weekly_stock_dict[
                "stock_close"].append(min_bar_dict["stock_close"])
            self.weekly_stock_dict["index_high"] = self.weekly_stock_dict[
                "index_high"].append(min_bar_dict["index_high"])
            self.weekly_stock_dict["index_low"] = self.weekly_stock_dict[
                "index_low"].append(min_bar_dict["index_low"])
            self.weekly_stock_dict["index_close"] = self.weekly_stock_dict[
                "index_close"].append(min_bar_dict["index_close"])
        else:
            # 如果当周有数据，进行实时更新
            # 1. 首先对缺失值进行填充
            self.weekly_stock_dict["stock_high"].iloc[-1] = (
                self.weekly_stock_dict["stock_high"].iloc[-1].fillna(
                    min_bar_dict["stock_high"]))
            self.weekly_stock_dict["stock_low"].iloc[-1] = (
                self.weekly_stock_dict["stock_low"].iloc[-1].fillna(
                    min_bar_dict["stock_low"]))
            self.weekly_stock_dict["stock_close"].iloc[-1] = (
                self.weekly_stock_dict["stock_close"].iloc[-1].fillna(
                    min_bar_dict["stock_close"]))
            self.weekly_stock_dict["index_high"].iloc[-1] = (
                self.weekly_stock_dict["index_high"].iloc[-1].fillna(
                    min_bar_dict["index_high"]))
            self.weekly_stock_dict["index_low"].iloc[-1] = (
                self.weekly_stock_dict["index_low"].iloc[-1].fillna(
                    min_bar_dict["index_low"]))
            self.weekly_stock_dict["index_close"].iloc[-1] = (
                self.weekly_stock_dict["index_close"].iloc[-1].fillna(
                    min_bar_dict["index_close"]))
            # 2. 更新
            self.weekly_stock_dict["stock_high"].iloc[-1] = pd.concat(
                [
                    self.weekly_stock_dict["stock_high"].iloc[-1],
                    min_bar_dict["stock_high"].rename("stock_high"),
                ],
                axis=1,
            ).max(axis=1)
            self.weekly_stock_dict["stock_low"].iloc[-1] = pd.concat(
                [
                    self.weekly_stock_dict["stock_low"].iloc[-1],
                    min_bar_dict["stock_low"].rename("stock_low"),
                ],
                axis=1,
            ).min(axis=1)
            self.weekly_stock_dict["stock_close"].iloc[-1] = min_bar_dict[
                "stock_close"]
            self.weekly_stock_dict["index_high"].iloc[-1] = pd.concat(
                [
                    self.weekly_stock_dict["index_high"].iloc[-1],
                    min_bar_dict["index_high"].rename("index_high"),
                ],
                axis=1,
            ).max(axis=1)
            self.weekly_stock_dict["index_low"].iloc[-1] = pd.concat(
                [
                    self.weekly_stock_dict["index_low"].iloc[-1],
                    min_bar_dict["index_low"],
                ],
                axis=1,
            ).min(axis=1)
            self.weekly_stock_dict["index_close"].iloc[-1] = min_bar_dict[
                "index_close"]

    # ---------------------------------------------------------------------------
    def update_factors(self):
        """
        更新技术指标
        """
        # 均线计算
        self.A = self.weekly_stock_dict["stock_close"].rolling(5).mean()
        self.B = self.weekly_stock_dict["stock_close"].rolling(20).mean()
        if "000001" in self.A.columns:
            self.B = pd.concat(
                [
                    self.B,
                    pd.DataFrame(columns=self.A.columns).drop(["000001"],
                                                              axis=1)
                ],
                axis=1,
                sort=True,
            ).fillna(
                method="pad", axis=1)
        else:
            self.B = (pd.concat(
                [self.B, pd.DataFrame(columns=self.A.columns)],
                axis=1,
                sort=True).fillna(method="pad", axis=1)).drop(["000001"],
                                                              axis=1)

    def update_signals(self, bar_time):
        """
        更新交易信号
        return: stock_list
        """
        a_direction = self.A.diff(1)
        b_direction = self.B.diff(1)

        first_cond = (self.A > self.B) & (a_direction > 0)
        first_res = first_cond.loc[self.current_weekly_trade_date]
        stock_list = sorted(first_res.loc[first_res].index.tolist())

        # TEST: 写入触发信号股票与相应 bar 时间
        if stock_list:
            print(bar_time + " 触发信号, 股票为 " + ",".join(stock_list))
            if not os.path.exists("{}/weekly_trigger.txt".format(
                    self.current_trade_date)):
                with open(
                        "{}/weekly_trigger.txt".format(
                            self.current_trade_date), "w") as f_in:
                    f_in.write(bar_time + "\t" + ",".join(stock_list) +
                               "\n")
            else:
                with open(
                        "{}/weekly_trigger.txt".format(
                            self.current_trade_date), "a") as f_in:
                    f_in.write(bar_time + "\t" + ",".join(stock_list) +
                               "\n")

        return stock_list

    def create_buy_orders(self, min_bar_dict):
        """
        创建买单
        """
        time_list = []
        stock_list = []
        while not self.order_queue.empty():
            value = self.order_queue.get()
            time_list.append(value["bar_time"])
            stock_list.append(value["stock_code"])
        buy_orders = pd.DataFrame()
        buy_orders["stock_code"] = stock_list
        buy_orders["bar_time"] = time_list
        buy_orders["order_direction"] = 1
        buy_orders = pd.merge(
            buy_orders,
            pd.DataFrame(min_bar_dict["stock_vmp"].rename("order_price")).reset_index(),
            how="left",
            on="stock_code",
        )
        # buy_orders["bar_time"] = min_bar_dict["bar_time"]
        buy_orders["order_price"] = np.round(buy_orders["order_price"], 2)
        buy_orders["order_amount"] = np.round(
            AMOUNT_PER_ORDER / buy_orders["order_price"] / 100.0) * 100

        buy_orders["trade_date"] = self.current_trade_date
        buy_orders["position_dates"] = 0
        buy_orders["order_life_minutes"] = BUY_ORDER_LIFE_MINUTES

        # 当日已发委托股票过滤
        buy_orders = buy_orders.loc[~buy_orders["stock_code"].
                                    isin(self.on_trade_stocks)]
        updated_stock_list = buy_orders.stock_code.tolist()
        buy_orders["A"] = self.A.iloc[
            -1][updated_stock_list].values
        buy_orders["B"] = self.B.iloc[
            -1][updated_stock_list].values

        if buy_orders.shape[0] != 0:
            self.on_trade_stocks = self.on_trade_stocks | set(
                updated_stock_list)

        # 添加新订单
        # print("添加新订单", buy_orders)
        self.order_book = self.order_book.append(buy_orders)

        # 每更新一次 order_book, 都记录下来
        # print("交易日与分钟bar信息"+self.current_trade_date+min_bar_dict["bar_time"].replace(":", ""))
        trades_info = buy_orders[["bar_time", "stock_code"]]
        # stock_list = trades_info.stock_code.tolist()
        # trades_info['A'] = self.A.loc[self.current_trade_date][stock_list].values
        # trades_info['B'] = self.B.loc[self.current_trade_date][stock_list].values
        # trades_info['C'] = self.C.loc[self.current_trade_date][stock_list].values
        # trades_info['D'] = self.D.loc[self.current_trade_date][stock_list].values
        # print("订单信息:", trades_info)
        if trades_info.shape[0] != 0:
            trades_info.to_csv(
                "{}/{}_buys.csv".format(self.current_trade_date, min_bar_dict["bar_time"].replace(":", "")),
                index=False)

    def update_position_pnl(self, min_bar_dict):
        """
        更新持仓盈亏
        说明：
        聚宽行情源默认停牌股票数据以未停牌时数据进行填充
        """

        self.account_position["bar_time"] = self.bar_time
        last_price = (pd.DataFrame(
            min_bar_dict["stock_close"].rename("last_price")).reset_index().rename(
                columns={
                    "symbol": "stock_code",
                }))
        # print("last_price:", last_price)
        self.account_position = pd.merge(
            self.account_position.drop("last_price", axis=1),
            last_price,
            on="stock_code",
            how="left",
        )
        # print(self.account_position)
        self.account_position["pnl_rate"] = np.round(
             self.account_position["last_price"] /
             self.account_position["cost_price"] - 1.0,
             4,
        )

    def create_sell_order(self, min_bar_dict):
        """
        创建卖单，包括：
        1. 盈亏止损
        2. 宽基止损
        3. 信号止损
        4. 正常卖出
        """
        # 1. 盈亏止损
        stop_loss_position_1 = self.account_position.loc[
            (self.account_position.position_dates >= 1)
            & (self.account_position.position_amount > 1)
            & (self.account_position["pnl_rate"] < -STOP_LOSS_PCT)]
        stop_loss_position_1["bar_time"] = self.bar_time
        stop_loss_position_1["order_direction"] = -1
        stop_loss_position_1["trade_date"] = self.current_trade_date
        stop_loss_position_1["order_amount"] = stop_loss_position_1[
            "position_amount"]
        stop_loss_position_1 = pd.merge(
            stop_loss_position_1,
            pd.DataFrame(
                min_bar_dict["stock_close"].rename("stock_close")).reset_index().rename(
                    columns={
                        "symbol": "stock_code",
                    }),
            on="stock_code",
            how="left",
        )
        stop_loss_position_1["order_price"] = stop_loss_position_1[
            "stock_close"]
        stop_loss_position_1["trade_amount"] = 0.0
        stop_loss_position_1["trade_price"] = 0.0
        stop_loss_position_1["trade_money"] = 0.0
        # 止损单默认三分钟出清
        stop_loss_position_1[
            "order_life_minutes"] = STOP_LOSS_ORDER_LIFE_MINUTES
        # 已经在订单中，不再做处理
        stop_loss_position_1 = stop_loss_position_1.loc[
            ~stop_loss_position_1.stock_code.isin(self.on_trade_stocks)]
        if stop_loss_position_1.shape[0] > 0:
            stop_loss_position_1.loc[stop_loss_position_1.trade_amount > 0].to_csv("{}/{}_stop_loss_1.csv".format(self.current_trade_date, self.bar_time.replace(":", "")))
            self.order_book = self.order_book.append(stop_loss_position_1)
            self.on_trade_stocks = self.on_trade_stocks | set(
                stop_loss_position_1.stock_code.tolist())

    #    # 2. 收盘前卖出
    #     if self.bar_time == SELL_BAR_TIME:
    #         sell_position = self.account_position.loc[
    #             (self.account_position.position_dates >= HOLD_POSITION_WEEKS)
    #             & (self.account_position.position_amount > 1)]
    #         sell_position = sell_position.loc[~sell_position.stock_code.
    #                                           isin(self.on_trade_stocks)]
    #         sell_position["order_direction"] = -1
    #         sell_position["order_amount"] = sell_position["position_amount"]
    #         sell_position = pd.merge(
    #             sell_position,
    #             pd.DataFrame(
    #                 self.min_bar_dict["stock_close"]).reset_index().rename(
    #                     columns={
    #                         "symbol": "stock_code",
    #                         self.current_trade_date: "order_price",
    #                     }),
    #             on="stock_code",
    #             how="left",
    #         )
    #         sell_position["trade_amount"] = 0
    #         sell_position["trade_price"] = 0
    #         sell_position["trade_money"] = 0
    #         sell_position["order_life_minutes"] = SELL_ORDER_LIFE_MINUTES
    #         sell_position["bar_time"] = self.bar_time
    #         sell_position["trade_date"] = self.current_trade_date
    #         if sell_position.shape[0] > 0:
    #             sell_position.loc[sell_position.trade_amount > 0].to_csv("{}/{}_sell.csv".format(self.current_trade_date, self.bar_time.replace(":", "")))
    #             self.order_book = self.order_book.append(sell_position)
    #             self.on_trade_stocks = self.on_trade_stocks | set(
    #                 sell_position.stock_code.tolist())


if __name__ == "__main__":
    strategy = Strategy()
    strategy.on_market_open()
    strategy.on_bar()
    strategy.on_market_close()
