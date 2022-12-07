# -*- coding: utf-8 -*-

from xtquant import xtdata
import aiohttp, akshare, re, datetime, math, requests, pytz
import pandas as pd
import numpy as np
from sanic import Sanic, Blueprint, response
import pandas_market_calendars as mcal
from collections import defaultdict
import pandas_ta as ta
from tqdm import tqdm
from copy import copy


api = Blueprint('xtdata', url_prefix='/api/xtdata')

FIRST_DATETIME_1D = '20210501093000'
FIRST_DATETIME_1M = '20221001093000'

# 记录bar最后时间，以减少重复入库
g_last_timestamp = {}
# 记录订阅并控制重复
g_subscribe_ids = {}

Exchange_XT2VT = {'SH':'SSE', 'SZ':'SZSE', 'HK':'SEHK', 'ZF':'CZCE', 'DF':'DCE', 'IF':'CFFEX', 'SF':'SHFE'}
Exchange_VT2XT = {'SSE':'SH', 'SZSE':'SZ', 'SEHK':'HK', 'CZCE':'ZF', 'DCE':'DF', 'CFFEX':'IF', 'SHFE':'SF'}

import time, asyncio
from vnpy_mysql.mysql_database import MysqlDatabase, BarGeneratorEx, BarData, Exchange, Interval, DB_TZ
# 可能需要每天重启，保证数据库链接有效
mdb = MysqlDatabase()

# 启动就订阅关注股票
from mysql.connector import connect
g_mysql_conn = connect(user='admin', password='admin', host='192.168.12.110', database='st', port='3309')
with g_mysql_conn.cursor() as _cursor:
    sql = "select s_code,display_name,list_dt,delist_dt,s_type,s_group from tb_monitor_security where s_type in ('lof','etf','stock');"
    _cursor.execute(sql)
    all = _cursor.fetchall()
    df = pd.DataFrame(all, columns=['s_code','display_name','begin_date','end_date','s_type','s_group'])
    g_subs_stock_list = ["000001.SH"]
    for s in df['s_code'].tolist():
        c,e = s.split('.')
        g_subs_stock_list.append(c + '.' + Exchange_VT2XT[e])


def save_local_bar_data(ticker, period, start_time, end_time, dividend_type='front'):
    """"""
    kv = xtdata.get_local_data(stock_code=[ticker], period=period, start_time=start_time, end_time=end_time, dividend_type=dividend_type)
    save_bar_data(kv, ticker=ticker, period=period)

def save_market_bar_data(ticker, period, start_time, end_time, dividend_type='front'):
    """"""
    kv = xtdata.get_market_data(stock_list=[ticker], period=period, start_time=start_time, end_time=end_time, dividend_type=dividend_type)
    save_bar_data(kv, ticker=ticker, period=period)

def save_bar_data(kv, ticker, period):
    """"""
    if len(kv) > 0:
        print(kv['close'])
        time_df: pd.DataFrame() = kv['time'].T[ticker]
        open_df: pd.DataFrame() = kv['open'].T[ticker]
        high_df: pd.DataFrame() = kv['high'].T[ticker]
        low_df: pd.DataFrame() = kv['low'].T[ticker]
        close_df: pd.DataFrame() = kv['close'].T[ticker]
        volume_df: pd.DataFrame() = kv['volume'].T[ticker]
        amount_df: pd.DataFrame() = kv['amount'].T[ticker]
        openInterest_df: pd.DataFrame() = kv['openInterest'].T[ticker]
        # print(openInterest_df)
        #
        bar_list = []

        symbol,exchange = ticker.split('.')
        for i in range(0, len(time_df)):
            #
            if time_df.iloc[i] == 0:
                continue
            data = {}
            data['time'] = time_df.iloc[i]
            data['open'] = open_df.iloc[i]
            data['high'] = high_df.iloc[i]
            data['low'] = low_df.iloc[i]
            data['close'] = close_df.iloc[i]
            data['volume'] = volume_df.iloc[i]
            data['amount'] = amount_df.iloc[i]
            data['openInterest'] = openInterest_df.iloc[i]
            #
            time_stamp = data['time'] / 1000 # 毫秒变秒
            interval = period[-1] if period[-1] in ('d','w') else period
            bar: BarData = BarData(
                        symbol=symbol,
                        exchange=Exchange(Exchange_XT2VT[exchange]),
                        datetime=datetime.datetime.fromtimestamp(time_stamp, DB_TZ),
                        interval=Interval(interval),
                        volume=data['volume'],
                        turnover=data['amount'],
                        open_interest=data['openInterest'],
                        open_price=data['open'],
                        high_price=data['high'],
                        low_price=data['low'],
                        close_price=data['close'],
                        gateway_name="DB"
                    )
            bar_list.append(bar)
        #
        if len(bar_list) > 0:
            try:
                print(ticker, period, len(bar_list))
                bar60_list = []
                if len(bar_list) % 241 == 0:
                    bars_list = [bar_list[i:i+241] for i in range(0,len(bar_list),241)]
                    for bars in bars_list:
                        bge = BarGeneratorEx(window=60, window_bar=bars[0], on_window_bar=lambda x:bar60_list.append(x))
                        for bar in bars[1:]:
                            bge.update_bar(bar)
                elif len(bar_list) % 240 == 0:
                    bars_list = [bar_list[i:i+240] for i in range(0,len(bar_list),240)]
                    for bars in bars_list:
                        bge = BarGeneratorEx(window=60, on_window_bar=lambda x:bar60_list.append(x))
                        for bar in bars:
                            bge.update_bar(bar)
                if len(bar60_list) > 0:
                    print(len(bar60_list), bar60_list)
                    mdb.save_bar_data(bar60_list)
                mdb.save_bar_data(bar_list)
            except Exception as e:
                raise e

#
def onSubscribe(datas):
    global g_last_timestamp
    # print(datas)
    if len(datas) > 0:
        for vt_symbol,datalist in datas.items():
            symbol,exchange = vt_symbol.split('.')
            bar_list = []
            for data in datalist:
                time_stamp = data['time'] / 1000 # 毫秒变秒
                bar: BarData = BarData(
                            symbol=symbol,
                            exchange=Exchange(Exchange_XT2VT[exchange]),
                            datetime=datetime.datetime.fromtimestamp(time_stamp, DB_TZ),
                            interval=Interval("1m"),
                            volume=data['volume'],
                            turnover=data['amount'],
                            open_interest=data['openInterest'],
                            open_price=data['open'],
                            high_price=data['high'],
                            low_price=data['low'],
                            close_price=data['close'],
                            gateway_name="DB"
                        )
                # 没有就入库，有比较时间戳
                if vt_symbol not in g_last_timestamp:
                    # 第一个就认为最后一个入库，防止无后续bar
                    last_time, last_bar = data['time'], bar
                    g_last_timestamp[vt_symbol] = (data['time'], bar)
                else:
                    last_time, last_bar = g_last_timestamp[vt_symbol]
                    g_last_timestamp[vt_symbol] = (data['time'], bar)
                    # 同一个时间，不是最后一个不入库
                    if data['time'] <= last_time:
                        continue
                bar_list.append(copy(last_bar))
            #
            if len(bar_list) > 0:
                print("onSubscribe:", vt_symbol, last_time)
                mdb.save_bar_data(bar_list)
                pass

@api.listener('before_server_start')
async def before_server_start(app, loop):
    '''全局共享session'''
    global session, cn_calendar, cn_tz, g_subscribe_ids, hs300_component, csi500_component, csi1000_component, all_a_tickers, last_day_price
    jar = aiohttp.CookieJar(unsafe=True)
    session = aiohttp.ClientSession(cookie_jar=jar, connector=aiohttp.TCPConnector(ssl=False))
    cn_calendar = mcal.get_calendar('SSE')
    cn_tz = pytz.timezone('Asia/Shanghai')
    id = xtdata.subscribe_whole_quote(['SH', 'SZ', 'HK'])
    g_subscribe_ids["all"] = id
    prev_dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d161100")
    end_dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    curr_time = datetime.datetime.now().strftime("%H%M%S")
    for stock_code in g_subs_stock_list:
        if "091500" < curr_time < "161100":
            xtdata.download_history_data(stock_code=stock_code, period="1m", start_time=prev_dt, end_time=end_dt)
            save_market_bar_data(ticker=stock_code, period="1m", start_time=prev_dt, end_time=end_dt)
        id = xtdata.subscribe_quote(stock_code, period="1m", start_time="", end_time="", count=1, callback=onSubscribe)
        g_subscribe_ids[stock_code] = id
    hs300_component, csi500_component, csi1000_component = get_a_index_component()
    all_a_tickers = get_all_a_tickers()
    # last_day_price = get_last_day_price(stock_ticker)

@api.listener('after_server_stop')
async def after_server_stop(app, loop):
    '''关闭session'''
    global g_subscribe_ids
    for seq_num in g_subscribe_ids.values():
        xtdata.unsubscribe_quote(seq_num)
    await session.close()

async def req_json(url):
    async with session.get(url) as resp:
        return await resp.json()

def get_a_index_etf():
    '''
    获取 上证指数、深证成指、创业板指、科创50、上证50、沪深300、中证500、中证1000等主要指数ticker，以及活跃ETFticker
    '''
    indexes = ['000001.SH', '399001.SZ', '399006.SZ', '000688.SH', '000016.SH', '000300.SH', '000905.SH', '000852.SH']
    etf = ["512100.SH", "510050.SH", "510300.SH", "513050.SH", "515790.SH", "563000.SH", "588000.SH", "513180.SH", "513060.SH", "159915.SZ", "512880.SH", "512010.SH", "512660.SH", "159949.SZ", "510500.SH", "512690.SH", "518880.SH", "511260.SH", "512480.SH", "512200.SH", "515030.SH", "511380.SH", "512000.SH", "510330.SH", "513130.SH", "513500.SH", "513100.SH", "512800.SH", "512760.SH", "159920.SZ", "159605.SZ", "159941.SZ", "162411.SZ", "513330.SH", "510900.SH", "513090.SH", "513550.SH"]
    return indexes + etf 

def get_a_index_component():
    '''
    获取沪深300(000300)、中证500(000905)、中证1000(000852)指数成分股
    '''
    hs300 = akshare.index_stock_cons_weight_csindex(symbol="000300")
    hs300['stock'] = hs300.apply(lambda row: row['成分券代码'] + '.' + {'上海证券交易所' :'SH', '深圳证券交易所': 'SZ'}.get(row['交易所']), axis=1)
    csi500 = akshare.index_stock_cons_weight_csindex(symbol="000905")
    csi500['stock'] = csi500.apply(lambda row: row['成分券代码'] + '.' + {'上海证券交易所' :'SH', '深圳证券交易所': 'SZ'}.get(row['交易所']), axis=1)
    csi1000 = akshare.index_stock_cons_weight_csindex(symbol="000852")
    csi1000['stock'] = csi1000.apply(lambda row: row['成分券代码'] + '.' + {'上海证券交易所' :'SH', '深圳证券交易所': 'SZ'}.get(row['交易所']), axis=1)

    hs300_component = hs300.set_index('stock')[['指数代码', '成分券名称', '权重']].to_dict('index')
    csi500_component = csi500.set_index('stock')[['指数代码', '成分券名称', '权重']].to_dict('index')
    csi1000_component = csi1000.set_index('stock')[['指数代码', '成分券名称', '权重']].to_dict('index')

    return hs300_component, csi500_component, csi1000_component

def get_etf_option():
    '''
    获取50ETF、300ETF对应的当月/次月期权ticker
    '''
    select_option = {}

    etf_price = {}
    # 获取ETF行情
    etf_tick = xtdata.get_full_tick(['510300.SH', '510050.SH', '159919.SZ'])
    # 取今日开盘价/昨日收盘价均值
    for code in ['510300.SH', '510050.SH', '159919.SZ']:
        etf_price[code] = (etf_tick[code]['open'] + etf_tick[code]['lastClose']) / 2

    options = xtdata.get_stock_list_in_sector('上证期权') + xtdata.get_stock_list_in_sector('深证期权')
    # 获取主力期权(标的价格附近上下5档,当月/次月)
    for code in options:
        meta = xtdata.get_instrument_detail(code)
        # 期权名称
        name = meta['InstrumentName']
        # 对应的ETF
        etf = re.findall(r'\((\d+)\)', meta['ProductID'])[0] 
        etf = {'510300': '510300.SH', '510050': '510050.SH', '159919': '159919.SZ'}.get(etf)
        # 剩余有效日
        days = (datetime.date(year=int(str(meta['ExpireDate'])[:4]), month=int(str(meta['ExpireDate'])[4:6]), day=int(str(meta['ExpireDate'])[6:8])) - datetime.date.today()).days
        call_put = 'call' if '购' in name else 'put'
        if days < 32:
            if math.fabs(etf_price[etf] - int(name[-4:]) / 1000.0) < 0.2:
                select_option[code] = [etf, call_put, int(name[-4:]), days]
        elif days < 65:
            if math.fabs(etf_price[etf] - int(name[-4:]) / 1000.0) < 0.25:
                select_option[code] = [etf, call_put, int(name[-4:]), days]

    return select_option

def get_a_future_contract():
    '''
    获取中金所、大商所、郑商所、上期所的主要连续合约
    '''
    contract = xtdata.get_stock_list_in_sector('中金所') + xtdata.get_stock_list_in_sector('大商所') + xtdata.get_stock_list_in_sector('郑商所') + xtdata.get_stock_list_in_sector('上期所') 
    market_mapping = {'CZCE': 'ZF', 'DCE': 'DF', 'CFFEX': 'IF', 'SHFE': 'SF'}
    print(contract)
    contract_main = [i.split('.')[0] + '.' + market_mapping.get(i.split('.')[1]) for i in contract if re.search(r'[A-Za-z]00\.[A-Z]', i)]
    if 'IM00.IF' not in contract_main:
        contract_main.append('IM00.IF')
    return contract_main

def get_a_cffex_contract():
    '''
    获取中金所股指期货和期权合约(上证50、沪深300[期权IO]、中证500、中证1000[期权MO])
    交割日期：每月第三个周五，遇节假日顺延
    '''
    trade_month = []
    today = datetime.date.today()
    this_month = datetime.date(today.year, today.month, 1)
    next_month1 = this_month + datetime.timedelta(days=31)
    next_month2 = this_month + datetime.timedelta(days=62)

    cnt = 0
    temp_date = None
    for i in pd.date_range(this_month, next_month1, freq='D'):
        if i.weekday() == 4:
            cnt += 1
        if cnt == 3:
            temp_date = i
            break

    month0_reverso_date = cn_calendar.schedule(start_date=temp_date, end_date=next_month1, tz=cn_tz).index.tolist()[0].date()
    if today > month0_reverso_date:
        # 本月交割日期之后，选择次月、次次月
        trade_month = ['{}{}'.format(next_month1.year % 100, str(next_month1.month).zfill(2)), '{}{}'.format(next_month2.year % 100, str(next_month2.month).zfill(2))]
    else:
        trade_month = ['{}{}'.format(this_month.year % 100, str(this_month.month).zfill(2)), '{}{}'.format(next_month1.year % 100, str(next_month1.month).zfill(2))]

    future_main_contract = ['{}{}.IF'.format(i, j) for i in ['IH', 'IF', 'IC', 'IM'] for j in trade_month]
    return future_main_contract, trade_month

def get_global_future_contract():
    '''
    外盘期货市场主要标的，包含汇率、利率、商品以及股指
    '''
    forex = ['DXY.OTC', 'EURUSD.OTC', 'GBPUSD.OTC', 'USDJPY.OTC', 'USDRUB.OTC', 'USDCNH.OTC', 'USDHKD.OTC']
    interest = ['US10YR.OTC', 'DE10YR.OTC', 'UK10YR.OTC', 'CN10YR.OTC', 'JP10YR.OTC', 'US5YR.OTC', 'US2YR.OTC', 'US1YR.OTC', 'US30YR.OTC', 'FR10YR.OTC', 'CN5YR.OTC', 'CN2YR.OTC', 'CN1YR.OTC', 'CN7YR.OTC']
    commodity = ['USHG.OTC', 'UKAH.OTC', 'UKCA.OTC', 'UKNI.OTC', 'UKPB.OTC', 'UKZS.OTC', 'UKSN.OTC', 'USZC.OTC', 'USZW.OTC', 'USYO.OTC', 'USZS.OTC', 'USLHC.OTC', 'UKOIL.OTC', 'USCL.OTC', 'USNG.OTC', 'XAUUSD.OTC', 'USGC.OTC', 'XAGUSD.OTC', 'USSI.OTC', 'AUTD.SGE', 'AGTD.SGE', 'PT9995.SGE', 'USPL.OTC', 'USPA.OTC']
    index = ["US500F.OTC", "VIXF.OTC", "US30F.OTC", "USTEC100F.OTC", "JP225F.OTC", "EU50F.OTC", "DE30F.OTC", "FR40F.OTC", "ES35F.OTC", "AU200F.OTC", "STOXX50F.OTC"]
    return forex + interest + commodity + index

def get_hk_index_comonent():
    '''
    获取恒生指数、恒生科技指数的成分股
    '''
    url = 'https://quotes.sina.cn/hq/api/openapi.php/HK_StockRankService.getHkStockList?page=1&num=100&symbol={}&asc=0&sort=changepercent&type={}'
    headers = {"user-agent": "sinafinance__6.4.0__iOS__248f2d8bf77fb1696a52f1bd4a55c1a256e54711__15.6__iPhone 11 Pro", "Host": "quotes.sina.cn", "accept-language": "zh-CN,zh-Hans;q=0.9"}

    component = {"HSI": {}, "HSTECH": {}}
    for index in ["HSI", "HSTECH"]:
        data = requests.get(url.format(index, index), headers=headers)
        try:
            data = data.json()['result']['data']['data']
            for item in data:
                component[index][item['symbol'] + '.HK'] = [index, item['name'], float(item['weight'])]
        except:
            pass
    return component["HSI"], component["HSTECH"]

@api.route('/subscribe', methods=['GET'])
async def subscribe(request, ticker_input=''):
    '''
    订阅单股行情: 获得tick/kline行情
    '''
    global g_subscribe_ids
    if ticker_input == '':
        ticker = request.args.get("ticker", "000001.SH")
    else:
        ticker = ticker_input
    period = request.args.get("period", "1m")
    if period == "1m":
        start_time = request.args.get("start_time", FIRST_DATETIME_1M)
    else:
        start_time = request.args.get("start_time", FIRST_DATETIME_1D)
    end_time = request.args.get("end_time", "")
    # 交易时间订阅，要补充历史数据
    if ticker not in g_subscribe_ids:
        last_time_str = datetime.datetime.now().strftime("%H:%M")
        if "09:15" < last_time_str < "15:15":
            prev_dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d161100")
            end_dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            xtdata.download_history_data(stock_code=ticker, period=period, start_time=prev_dt, end_time=end_dt)
            save_local_bar_data(ticker=ticker, period=period, start_time=prev_dt, end_time=end_dt)
            save_market_bar_data(ticker=ticker, period=period, start_time=prev_dt, end_time=end_dt)
        id = xtdata.subscribe_quote(ticker, period, start_time=start_time, end_time=end_time, count=1, callback=onSubscribe)
        g_subscribe_ids[ticker] = id
    else:
        prev_dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d161100")
        end_dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # 当天的数据每次订阅都重写
        save_local_bar_data(ticker=ticker, period=period, start_time=prev_dt, end_time=end_dt)
        save_market_bar_data(ticker=ticker, period=period, start_time=prev_dt, end_time=end_dt)
        id = g_subscribe_ids[ticker]
    if ticker_input == '':
        return response.json({"ticker": ticker, "data": id})
    else:
        return {"ticker": ticker, "data": id}

@api.route('/download/history_data', methods=['GET'])
async def download_history_data(request, ticker_input=''):
    '''
    批量下载: 获得tick/kline数据
    '''
    if ticker_input == '':
        ticker = request.args.get("ticker", "000001.SH")
    else:
        ticker = ticker_input
    period = request.args.get("period", "1m")
    if period == "1m":
        start_time = request.args.get("start_time", FIRST_DATETIME_1M)
    else:
        start_time = request.args.get("start_time", FIRST_DATETIME_1D)
    end_time = request.args.get("end_time", "")
    # 默认把下载数据存入自己数据库
    save_db = request.args.get("save_db", True)
    #
    xtdata.download_history_data(stock_code=ticker, period=period, start_time=start_time, end_time=end_time)
    if save_db:
        if end_time == "":
            prev_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d161100")
        else:
            prev_time = end_time
        # 没指定结束时间就取上一个日期收盘，以保证可生成1h数据
        save_local_bar_data(ticker=ticker, period=period, start_time=start_time, end_time=prev_time)
    return response.json({"download": ticker, "period": period})

@api.route('/get/local_data', methods=['GET'])
async def get_local_data(request):
    '''
    批量下载: 获得tick/kline数据
    '''
    ticker = request.args.get("ticker", "000001.SH")
    period = request.args.get("period", "1m")
    if period == "1m":
        start_time = request.args.get("start_time", FIRST_DATETIME_1M)
    else:
        start_time = request.args.get("start_time", FIRST_DATETIME_1D)
    end_time = request.args.get("end_time", "")

    kv = xtdata.get_local_data(stock_code=[ticker], period=period, start_time=start_time, end_time=end_time, dividend_type='front')
    data = {}
    for k,v in kv.items():
        print(k, v)
        data[k] = v.to_dict('list')
    return response.json({"download": ticker, "data": data})


def get_all_a_tickers():
    global index_ticker, stock_ticker
    # 沪深指数
    index_ticker = xtdata.get_stock_list_in_sector("沪深指数")
    # print("沪深指数", index_ticker)
    # 沪深A股
    stock_ticker = xtdata.get_stock_list_in_sector("沪深A股")
    # print("沪深A股", stock_ticker)
    # 沪深债券
    bond_ticker = xtdata.get_stock_list_in_sector("沪深债券")
    # print("沪深债券", bond_ticker)
    # 板块指数
    sector_ticker = xtdata.get_stock_list_in_sector("板块指数")
    # print("板块指数", sector_ticker)
    # 沪深基金
    fund_ticker = xtdata.get_stock_list_in_sector("沪深基金")
    # print("沪深基金", fund_ticker)
    tickers = index_ticker + stock_ticker + bond_ticker + sector_ticker + fund_ticker
    return tickers

@api.route('/download/kline/1m', methods=['GET'])
async def download_kline_1m(request):
    '''
    下载A股市场全部1分钟K线
    '''
    save_db = request.args.get("save_db", False)
    
    for ticker in tqdm(["000001.SH"] + stock_ticker):
        xtdata.download_history_data(stock_code=ticker, period='1m', start_time=FIRST_DATETIME_1M, end_time='')
        #
        if save_db:
            save_local_bar_data(ticker=ticker, period='1m', start_time=FIRST_DATETIME_1M, end_time='')
       
    return response.json({"data": len(stock_ticker)})

@api.route('/local/kline/1m', methods=['GET'])
async def local_kline_1m(request):
    '''
    本地更新A股市场全部1分钟K线
    '''
    prev_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d161100")
    for ticker in tqdm(["000001.SH"] + stock_ticker):
        save_local_bar_data(ticker=ticker, period='1m', start_time=FIRST_DATETIME_1M, end_time=prev_time)
       
    return response.json({"data": len(stock_ticker)})

@api.route('/download/kline/1d', methods=['GET'])
async def download_kline_1d(request):
    '''
    下载A股市场全部1分钟K线
    '''
    save_db = request.args.get("save_db", False)
    
    for ticker in tqdm(["000001.SH"] + stock_ticker):
        xtdata.download_history_data(stock_code=ticker, period='1d', start_time=FIRST_DATETIME_1D, end_time='')
        #
        if save_db:
            save_local_bar_data(ticker=ticker, period='1d', start_time=FIRST_DATETIME_1D, end_time='')
       
    return response.json({"data": len(stock_ticker)})

@api.route('/local/kline/1d', methods=['GET'])
async def local_kline_1d(request):
    '''
    本地更新A股市场全部1分钟K线
    '''
    for ticker in tqdm(["000001.SH"] + stock_ticker):
        save_local_bar_data(ticker=ticker, period='1d', start_time=FIRST_DATETIME_1D, end_time='')
       
    return response.json({"data": len(stock_ticker)})

@api.route('/download/basic_data', methods=['GET'])
async def download_basic_data(request):
    '''
    下载基础数据: 财务报表、板块分类
    '''
    print("下载板块分类信息... ",  xtdata.download_sector_data())
    print("下载指数成分权重信息... ",  xtdata.download_index_weight())
    print("下载历史合约... ",  xtdata.download_history_contracts())
    all_a_stock = xtdata.get_stock_list_in_sector('沪深A股') 
    print("下载财务数据... ",  xtdata.download_financial_data(all_a_stock))

@api.route('/quote/kline', methods=['GET'])
async def quote_kline(request, tickers=''):
    '''
    查询市场行情: 获得kline数据
    '''
    if tickers == '':
        tickers = request.args.get("tickers", "IM00.IF,159919.SZ,00700.HK,10004407.SHO")
    period = request.args.get("period", "1m")
    start_time = request.args.get("start_time", "")
    end_time = request.args.get("end_time", "")
    count = request.args.get("count", "1")
    dividend_type = request.args.get("dividend_type", "none") # none 不复权 front 前复权 back 后复权 front_ratio 等比前复权 back_ratio 等比后复权
    stock_list = tickers.split(',')

    kline_data = xtdata.get_market_data(field_list=['time', 'open', 'high', 'low', 'close', 'volume', 'amount'], stock_list=stock_list, period=period, start_time=start_time, end_time=end_time, count=int(count), dividend_type=dividend_type, fill_data=True)

    quote_data = {}
    for stock in stock_list:
        df = pd.concat([kline_data[i].loc[stock].T for i in ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']], axis=1)
        df.columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
        df = df[df.volume !=0]
        df['time'] = df['time'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
        df['ticker'] = stock
        df = df[['ticker', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount']].values.tolist() 
        quote_data[stock] = df

    return response.json({"data": quote_data})

@api.route('/quote/tick', methods=['GET'])
async def quote_tick(request):
    '''
    查询市场行情: 获得tick数据
    '''
    tickers = request.args.get("tickers", "159919.SZ,00700.HK")
    stock_list = tickers.split(',')
    data = xtdata.get_full_tick(stock_list)
    return response.json({"data": data})

@api.route('/subscribe/kline/hs300', methods=['GET'])
async def subscribe_kline_hs300(request):
    '''
    订阅市场行情: 沪深300成分股1分钟K线行情
    '''
    seq_ids = []
    for ticker in hs300_component:
       seq_id =  await subscribe(request, ticker_input=ticker)
       seq_ids.append(seq_id.get('data', -1))
    return response.json({"data": seq_ids})

@api.route('/get/future', methods=['GET'])
async def get_future(request):
    '''
    订阅市场行情: 沪深300成分股1分钟K线行情
    '''
    data = get_a_future_contract()
    return response.json({"data": data})


@api.route('/subscribe/kline/future', methods=['GET'])
async def subscribe_kline_future(request):
    '''
    订阅市场行情: 沪深300成分股1分钟K线行情
    '''
    seq_ids = []
    for ticker in tqdm(get_a_future_contract()):
       seq_id =  await subscribe(request, ticker_input=ticker)
       seq_ids.append(seq_id.get('data', -1))
    return response.json({"data": seq_ids})

@api.route('/subscribe/kline/global_future', methods=['GET'])
async def subscribe_kline_global_future(request):
    '''
    订阅市场行情: 沪深300成分股1分钟K线行情
    '''
    seq_ids = []
    for ticker in tqdm(get_global_future_contract()):
       seq_id =  await subscribe(request, ticker_input=ticker)
       seq_ids.append(seq_id.get('data', -1))
    return response.json({"data": seq_ids})

@api.route('/subscribe/kline/a_stock', methods=['GET'])
async def subscribe_kline_a_stock(request):
    '''
    订阅市场行情: 沪深300成分股1分钟K线行情
    '''
    seq_ids = []
    for ticker in ["000001.SH"] + stock_ticker:
       seq_id =  await subscribe(request, ticker_input=ticker)
       seq_ids.append(seq_id.get('data', -1))
    return response.json({"data": seq_ids})

@api.route('/quote/kline/hs300', methods=['GET'])
async def quote_kline_hs300(request):
    '''
    查询市场行情: 沪深300成分股1分钟K线行情
    '''
    return await quote_kline(request, ','.join(list(hs300_component)))

@api.route('/quote/instrument/detail', methods=['GET'])
async def get_instrument_detail(request):
    '''
    获取合约基础信息
    '''
    ticker = request.args.get("ticker", "159919.SZ")
    return response.json({"data": xtdata.get_instrument_detail(ticker)})

@api.route('/quote/sector/list', methods=['GET'])
async def get_sector_list(request):
    '''
    获取板块列表
    '''
    return response.json({"data": xtdata.get_sector_list()})

@api.route('/quote/sector/component', methods=['GET'])
async def get_sector_component(request):
    '''
    获取板块成分股列表
    '''
    sector_name = request.args.get("sector", "沪深ETF")
    return response.json({"data": xtdata.get_stock_list_in_sector(sector_name)})

def get_last_day_price(tickers=['159919.SZ', '510050.SH', '000810.SZ'], trade_day=datetime.date.today().strftime("%Y%m%d")):
    """"""
    tickers_list = [tickers[i:i+499] for i in range(0,len(tickers),499)]
    result = {}
    for ticker_b in tickers_list:
        ticker_b = ['000001.SH'] + ticker_b
        kline_data = xtdata.get_market_data(field_list=['close'], stock_list=ticker_b, period='1m', start_time='', end_time=trade_day + '080000', count=1, dividend_type='front', fill_data=True)
        print(kline_data)
        for ticker in ticker_b:
            result[ticker] = kline_data['close'].loc[ticker].values[0]
    return result

@api.route('/feature/tech', methods=['GET'])
async def feature_tech(request, tickers=''):
    '''
    计算实时技术特征
    '''
    if tickers == '':
        tickers = request.args.get("tickers", "159919.SZ,510050.SH,000810.SZ")
    stock_list = tickers.split(',')
    start_time = request.args.get("start_time", datetime.datetime.now().strftime("%Y%m%d000001"))
    end_time = request.args.get("end_time", datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

    kline_data = xtdata.get_market_data(field_list=['time', 'open', 'high', 'low', 'close', 'volume', 'amount'], stock_list=stock_list, period='1m', start_time=start_time, end_time=end_time)

    features = []
    for stock in stock_list:
        kline_df = pd.concat([kline_data[i].loc[stock].T for i in ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']], axis=1)
        kline_df.columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
        kline_df['trade_time'] = kline_df['time'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000.0))
        ticker= stock.split('.')[0] + {'SH': '.XSHG', 'SZ': '.XSHE'}.get(stock.split('.')[1])
        trade_date = kline_df['trade_time'].iloc[0].strftime("%Y-%m-%d")
        trade_time = int(kline_df['time'].iloc[-1] // 1000)
        kline_df['price_last'] = last_day_price.get(stock, None)
        kline_df['minute_avg'] = (kline_df['high'] + kline_df['low']) / 2
        kline_df['minutes_of_day'] = kline_df.trade_time.dt.hour * 60 + kline_df.trade_time.dt.minute

        kline_df['price_open'] = kline_df['minute_avg'].iloc[0]
        kline_df['pct_daily'] = (kline_df['close'] - kline_df['price_last']).div(kline_df['price_last'])
        kline_df['pct_intraday'] = (kline_df['close'] - kline_df['price_open']).div(kline_df['price_open'])

        pct_daily = kline_df['pct_daily'].iloc[-1]
        pct_intraday = kline_df['pct_intraday'].iloc[-1]
        rsi_3 = (kline_df.ta.rsi(length=3) / 100).fillna(0.5).iloc[-1]
        cmo_5 = (kline_df.ta.cmo(length=5) / 100).fillna(0.).iloc[-1]
        cmo_8 = (kline_df.ta.cmo(length=8) / 100).fillna(0.).iloc[-1]
        kdj_9_3 = (kline_df.ta.kdj(min(9, len(kline_df)), 3) / 100).fillna(0.5).iloc[-1].tolist() 
        willr_3 = (kline_df.ta.willr(length=3) / 100).clip(-1, 1).fillna(-0.5).iloc[-1]
        willr_5 = (kline_df.ta.willr(length=5) / 100).clip(-1, 1).fillna(-0.5).iloc[-1]
        willr_10 = (kline_df.ta.willr(length=min(10, len(kline_df))) / 100).clip(-1, 1).fillna(-0.5).iloc[-1]
        dpo_5 = (kline_df.ta.dpo(length=5, lookahead=False) * 10).clip(-3, 3).fillna(0.0).iloc[-1]
        log_return_10 = (kline_df.ta.log_return(length=10) * 10).clip(-3, 3).fillna(0.).iloc[-1]
        log_return_5 = (kline_df.ta.log_return(length=5) * 10).clip(-3, 3).fillna(0.).iloc[-1]
        log_return_3 = (kline_df.ta.log_return(length=3) * 10).clip(-3, 3).fillna(0.).iloc[-1]
        zscore_10 = (kline_df.ta.zscore(length=10)).clip(-3, 3).fillna(0.).iloc[-1]
        zscore_5 = (kline_df.ta.zscore(length=5)).clip(-3, 3).fillna(0.).iloc[-1]
        zscore_3 = (kline_df.ta.zscore(length=3)).clip(-3, 3).fillna(0.).iloc[-1]
        pct_volatility = (10 * (kline_df['high'] - kline_df['low']).div(kline_df['minute_avg'])).clip(-1, 1).fillna(0.).iloc[-1]
        rolling_pct_volatility_3 =  (20 * (kline_df['high'].rolling(3, min_periods=1).max() - kline_df['low'].rolling(3, min_periods=1).min()).div(kline_df['minute_avg'])).clip(-3, 3).fillna(0.).iloc[-1]
        rolling_pct_volatility_5 =  (20 * (kline_df['high'].rolling(5, min_periods=1).max() - kline_df['low'].rolling(5, min_periods=1).min()).div(kline_df['minute_avg'])).clip(-3, 3).fillna(0.).iloc[-1]
        rolling_pct_volatility_10 =  (20 * (kline_df['high'].rolling(10, min_periods=1).max() - kline_df['low'].rolling(10, min_periods=1).min()).div(kline_df['minute_avg'])).clip(-3.1, 3.1).fillna(0.).iloc[-1]

        feature = [ticker, trade_date, trade_time] +  (np.array([pct_daily, pct_intraday, rsi_3 , cmo_5, cmo_8] + kdj_9_3 +[willr_3, willr_5, willr_10, dpo_5, log_return_3, log_return_5, log_return_10, zscore_3, zscore_5, zscore_10, pct_volatility, rolling_pct_volatility_3, rolling_pct_volatility_5, rolling_pct_volatility_10]) * 10000).clip(-2**15, 2**15-1).round().tolist()
        features.append(feature)
    return response.json({"data": features})


# 定时任务函数
async def taskFunc(app):
    """"""
    global g_last_timestamp
    while True:
        await asyncio.sleep(30)
		# 执行最后一个bar入库
        if time.strftime("%H:%M") == "11:31" or time.strftime("%H:%M") == "15:01":
            # 遍历
            new_timestamp = {}
            for k,v in g_last_timestamp.items():
                if k.startswith('0') and len(k) == 8:
                    new_timestamp[k] = v
                    continue
                last_time, last_bar = v
                # 入库未入库bar
                if hasattr(last_bar, 'gateway_name'):
                    try:
                        print("taskFunc:", k, last_time)
                        mdb.save_bar_data([last_bar])
                    except Exception as e:
                        print(e)
                        pass
            # 没处理的港股标的
            g_last_timestamp = new_timestamp
        # 港股时间
        if time.strftime("%H:%M") == "12:01" or time.strftime("%H:%M") == "16:11":
            # 遍历
            for k,v in g_last_timestamp.items():
                if len(k) > 8: # 一般为空
                    print("error:", k)
                    continue
                last_time, last_bar = v
                # 入库未入库bar
                if hasattr(last_bar, 'gateway_name'):
                    try:
                        print("taskFunc:", k, last_time)
                        mdb.save_bar_data([last_bar])
                    except Exception as e:
                        print(e)
                        pass
            g_last_timestamp = {}
		# 执行前复权数据更新
        if time.strftime("%H:%M") == "09:01" or time.strftime("%H:%M") == "17:31":
            # 每天需要更新000001.SH这个数据，确保其他下载正常
            # period = "1m"
            prev_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d161100")
            for ticker in tqdm(["000001.SH"] + stock_ticker):
                xtdata.download_history_data(stock_code=ticker, period="1m", start_time=prev_time)
                xtdata.download_history_data(stock_code=ticker, period="1d", start_time=prev_time)
                #
                df = xtdata.get_divid_factors(ticker, start_time=prev_time, end_time='')
                if len(df) > 0:
                    save_local_bar_data(ticker=ticker, period="1m", start_time=FIRST_DATETIME_1M, end_time='')
                    save_local_bar_data(ticker=ticker, period="1d", start_time=FIRST_DATETIME_1D, end_time='')
                else:
                    save_local_bar_data(ticker=ticker, period="1m", start_time=prev_time, end_time='')
                    save_local_bar_data(ticker=ticker, period="1d", start_time=prev_time, end_time='')
        #
        if time.strftime("%H:%M") == "18:25":
            # 每天退出
            app.stop()
            exit(0)


app = Sanic(name='xtquant')
app.config.RESPONSE_TIMEOUT = 600000
app.config.REQUEST_TIMEOUT = 600000
app.config.KEEP_ALIVE_TIMEOUT = 6000
app.blueprint(api)

# 启动
app.add_task(taskFunc(app))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7800, workers=1, auto_reload=None, debug=True)