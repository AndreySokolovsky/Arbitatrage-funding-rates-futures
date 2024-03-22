import json
import requests
import time
import threading
from datetime import datetime

settings={
    'ts':30, #Время задержки в секундах
    'delta':0.5 #Разница между ставками финансирования
}

result={} #Словарь с результатами {'коин':'[[биржа,ставка],...,[биржа,ставка]]'}

def get_funding_rates_huobi(): # Получение тикеров ставок финансирования с биржи huobi
    try:
        response = json.loads((requests.get(url=f"https://api.hbdm.com/linear-swap-api/v1/swap_batch_funding_rate")).text)['data']
        rez = {r['contract_code'].upper()[:-5]: ['huobi',float(r['funding_rate'])*100] for r in response if r['contract_code'].endswith('USDT')}
        for k,v in rez.items():
            result.setdefault(k,[]).append(v)
        return f'huobi: OK --> {len(rez)} pairs'
    except Exception:
        return f'huobi error'


def get_funding_rates_kucoin(): # Получение тикеров ставок финансирования с биржи kucoin
    try:
        response = json.loads((requests.get(url=f"https://api-futures.kucoin.com/api/v1/contracts/active")).text)['data']
        rez = {r['symbol'].upper()[:-5]: ['kucoin',float(r['fundingFeeRate'])*100] for r in response if r['symbol'].endswith('USDTM')}
        for k,v in rez.items():
            result.setdefault(k,[]).append(v)
        return f'kucoin:  OK --> {len(rez)} pairs'
    except Exception:
        return f'kucoin error'

def get_funding_rates_coinex(): # Получение тикеров ставок финансирования с биржи coinex
    try:
        response = json.loads((requests.get(url=f"https://api.coinex.com/perpetual/v1/market/ticker/all")).text)['data']['ticker']
        rez = {k.upper()[:-4]: ['coinex',float(v['funding_rate_next'])*100] for k,v in response.items() if k.endswith('USDT')}
        for k,v in rez.items():
            result.setdefault(k,[]).append(v)
        return f'coinex:  OK --> {len(rez)} pairs'
    except Exception:
        return f'coinex error'

def get_funding_rates_xt(): # Получение тикеров ставок финансирования с биржи xt
    try:
        response = json.loads((requests.get(url=f"https://fapi.xt.com/future/market/v1/public/cg/contracts")).text)
        rez ={r['base_currency']:['xt',float(r['funding_rate'])*100] for r in response if r['target_currency'].endswith('USDT') and isinstance(r['funding_rate'],str) }
        for k,v in rez.items():
            result.setdefault(k,[]).append(v)
        return f'xt:  OK --> {len(rez)} pairs'
    except Exception as ex:
        return f'xt error'

def get_funding_rates_bingx(): # Получение тикеров ставок финансирования с биржи bingx
    try:
        response = json.loads((requests.get(url=f"https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex")).text)['data']
        rez ={
            r['symbol'].upper()[:-5]:
                  ['bingx',float(r['lastFundingRate'])*100]
              for r in response
              if r['symbol'].endswith('USDT')
              and isinstance(r['lastFundingRate'],str)
              }
        for k,v in rez.items():
            result.setdefault(k,[]).append(v)
        return f'bingx:  OK --> {len(rez)} pairs'
    except Exception as ex:
        return f'bingx error'

def get_funding_rates_bitget(): # Получение тикеров ставок финансирования с биржи bitget
    try:
        response = json.loads((requests.get(url=f"https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")).text)['data']
        rez ={r['symbol'].upper()[:-4]:['bitget',float(r['fundingRate'])*100] for r in response if r['symbol'].endswith('USDT') and isinstance(r['fundingRate'],str) }
        for k,v in rez.items():
            result.setdefault(k,[]).append(v)
        return f'bitget:  OK --> {len(rez)} pairs'
    except Exception as ex:
        return f'bitget error'

def get_funding_rates_mexc(): # Получение тикеров ставок финансирования с биржи mexc
    try:
        response = json.loads((requests.get(url=f"https://contract.mexc.com/api/v1/contract/ticker")).text)['data']
        rez ={r['symbol'].upper()[:-5]:['mexc',float(r['fundingRate'])*100] for r in response if r['symbol'].endswith('USDT') and isinstance(r['fundingRate'],float) }
        for k,v in rez.items():
            result.setdefault(k,[]).append(v)
        return f'mexc:  OK --> {len(rez)} pairs'
    except Exception as ex:
        return f'mexc error'

def get_funding_rates_binance(): # Получение тикеров ставок финансирования с биржи binance
    try:
        response = json.loads((requests.get(url=f"https://fapi.binance.com/fapi/v1/premiumIndex")).text)
        rez ={r['symbol'].upper()[:-4]:['binance',float(r['lastFundingRate'])*100] for r in response if r['symbol'].endswith('USDT') and isinstance(r['lastFundingRate'],str) }
        for k,v in rez.items():
            result.setdefault(k,[]).append(v)
        return f'binance:  OK --> {len(rez)} pairs'
    except Exception as ex:
        return f'binance error'

def get_funding_rates_gate(): # Получение тикеров ставок финансирования с биржи gate
    try:
        response = json.loads((requests.get(url=f"https://api.gateio.ws/api/v4/futures/usdt/tickers")).text)
        rez ={r['contract'].upper()[:-5]:['gate',float(r['funding_rate'])*100] for r in response if r['contract'].endswith('USDT') and isinstance(r['funding_rate'],str) }
        for k,v in rez.items():
            result.setdefault(k,[]).append(v)
        return f'gate:  OK --> {len(rez)} pairs'
    except Exception as ex:
        return f'gate error'

def get_funding_rates_bybit(): # Получение тикеров ставок финансирования с биржи bybit
    try:
        response = json.loads((requests.get(url=f"https://api.bybit.com/v5/market/tickers?category=linear&baseCoin=USDT")).text)['result']['list']
        rez ={r['symbol'].upper()[:-4]:['bybit',float(r['fundingRate'])*100] for r in response if r['symbol'].endswith('USDT') and isinstance(r['fundingRate'],str) }
        for k,v in rez.items():
            result.setdefault(k,[]).append(v)
        return f'bybit:  OK --> {len(rez)} pairs'
    except Exception as ex:
        return f'bybit error'


while True:
    p1 = threading.Thread(target=print(get_funding_rates_huobi()), daemon=True)
    p2 = threading.Thread(target=print(get_funding_rates_kucoin()), daemon=True)
    p3 = threading.Thread(target=print(get_funding_rates_coinex()), daemon=True)
    p4 = threading.Thread(target=print(get_funding_rates_xt()), daemon=True)
    p5 = threading.Thread(target=print(get_funding_rates_bingx()), daemon=True)
    p6 = threading.Thread(target=print(get_funding_rates_bitget()), daemon=True)
    p7 = threading.Thread(target=print(get_funding_rates_mexc()), daemon=True)
    p8 = threading.Thread(target=print(get_funding_rates_binance()), daemon=True)
    p9 = threading.Thread(target=print(get_funding_rates_gate()), daemon=True)
    p10 = threading.Thread(target=print(get_funding_rates_bybit()), daemon=True)
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    p10.start()
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()
    p10.join()
    print('_' * 30)

    for k,v in result.items():
        if len(v)>=2: #проверка присутствия коина на двух и более биржах
            max_rate=max(v,key = lambda x: x[1]) #Максимальная ставка
            min_rate=min(v,key = lambda x: x[1]) #Минимальная ставка
            if max_rate[1]-min_rate[1]>settings['delta']:
                print(f"{k}--{v}")
    #Информация о текущем времени
    print('_'*30)
    print(datetime.now())
    print('_'*30)
    result.clear()
    time.sleep(settings['ts'])