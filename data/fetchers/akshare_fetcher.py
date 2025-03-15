import logging
import akshare as ak
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
from .base_fetcher import BaseFetcher, DataFetchError
from tqdm import tqdm
from functools import partial

class AKShareFetcher(BaseFetcher):
    def __init__(self):
        super().__init__("akshare")
        self._market_mapping = {
            'CN': '.SZ|.SH',
            'HK': '.HK',
            'US': '',
        }
        self._cache = {}
        self.logger.setLevel(logging.DEBUG)
        self._market_functions = {
            'CN': ak.stock_zh_a_spot_em,
            'HK': ak.stock_hk_spot_em,
            'US': ak.stock_us_spot_em,
        }
        
        if hasattr(ak, 'set_tqdm'):
            ak.set_tqdm(False)
        for func in self._market_functions.values():
            if hasattr(func, '__globals__') and 'tqdm' in func.__globals__:
                func.__globals__['tqdm'] = lambda x, **kwargs: x

    async def _fetch_raw_market_data(self, symbol: str) -> Dict:
        """獲取原始市場數據"""
        try:
            self.logger.debug(f"開始獲取股票 {symbol} 的市場數據")
            market = self._detect_market(symbol)
            self.logger.debug(f"檢測到市場類型: {market}")
            
            # 獲取對應市場的數據獲取函數
            market_func = self._market_functions.get(market)
            if not market_func:
                raise DataFetchError(f"不支持的市場: {market}")
            
            data = market_func()
            self.logger.debug(f"成功獲取原始數據，形狀: {data.shape}")
            self.logger.debug(f"數據列: {data.columns.tolist()}")
                
            # 根據不同市場處理數據
            if market == 'CN':
                stock_code = symbol.split('.')[0]
                stock_data = data[data['代码'] == stock_code]
                if stock_data.empty:
                    raise DataFetchError(f"未找到股票 {symbol} 的數據")
                
                result = {
                    'price': float(stock_data['最新价'].iloc[0]),
                    'volume': int(stock_data['成交量'].iloc[0]),
                    'pe_ratio': float(stock_data['市盈率-动态'].iloc[0]),
                    'market_cap': float(stock_data['总市值'].iloc[0]),
                    'currency': 'CNY',
                    'timestamp': datetime.now().isoformat()
                }
                
            elif market == 'HK':
                stock_code = symbol.split('.')[0]
                stock_data = data[data['代码'] == stock_code]
                if stock_data.empty:
                    raise DataFetchError(f"未找到股票 {symbol} 的數據")
                
                result = {
                    'price': float(stock_data['最新价'].iloc[0]),
                    'volume': int(stock_data['成交量'].iloc[0]),
                    'pe_ratio': float(stock_data['市盈率'].iloc[0]),
                    'market_cap': float(stock_data['市值'].iloc[0]),
                    'currency': 'HKD',
                    'timestamp': datetime.now().isoformat()
                }
                
            elif market == 'US':
                stock_data = data[data['代码'] == symbol]
                if stock_data.empty:
                    raise DataFetchError(f"未找到股票 {symbol} 的數據")
                
                result = {
                    'price': float(stock_data['最新价'].iloc[0]),
                    'volume': int(stock_data['成交量'].iloc[0]),
                    'pe_ratio': float(stock_data['市盈率'].iloc[0]),
                    'market_cap': float(stock_data['市值'].iloc[0]),
                    'currency': 'USD',
                    'timestamp': datetime.now().isoformat()
                }
                
            elif market == 'JP':
                stock_code = symbol.split('.')[0]
                stock_data = data[data['代码'] == stock_code]
                if stock_data.empty:
                    raise DataFetchError(f"未找到股票 {symbol} 的數據")
                
                result = {
                    'price': float(stock_data['最新价'].iloc[0]),
                    'volume': int(stock_data['成交量'].iloc[0]),
                    'pe_ratio': float(stock_data['市盈率'].iloc[0]),
                    'market_cap': float(stock_data['市值'].iloc[0]),
                    'currency': 'JPY',
                    'timestamp': datetime.now().isoformat()
                }
            
            self.logger.debug(f"處理後的數據: {result}")
            return result

        except Exception as e:
            self.logger.error(f"獲取市場數據時發生錯誤: {str(e)}", exc_info=True)
            raise DataFetchError(f"獲取市場數據失敗: {str(e)}")
        
    def _detect_market(self, symbol: str) -> str:
        """根據股票代碼判斷市場"""
        self.logger.debug(f"正在判斷股票 {symbol} 所屬市場")
        for market, suffix in self._market_mapping.items():
            if suffix:
                self.logger.debug(f"檢查是否匹配 {market} 市場 (後綴: {suffix})")
                if any(symbol.endswith(s) for s in suffix.split('|')):
                    self.logger.debug(f"匹配到市場: {market}")
                    return market
            elif '.' not in symbol:  # 美股情況
                self.logger.debug(f"檢測到美股代碼: {symbol}")
                return market
                
        error_msg = f"無法識別股票代碼所屬市場: {symbol}"
        self.logger.error(error_msg)
        raise DataFetchError(error_msg)

    async def is_available(self) -> bool:
        """檢查數據源是否可用"""
        try:
            self.logger.debug("正在測試 AKShare 連接...")
            ak.stock_zh_a_spot_em()
            self.logger.debug("AKShare 連接測試成功")
            return True
        except Exception as e:
            self.logger.error(f"AKShare 連接測試失敗: {str(e)}", exc_info=True)
            return False
        
    def _process_cn_financials(self, data) -> Dict:
        """處理A股財務數據"""
        return {
            'revenue': float(data['营业收入'].iloc[-1]),
            'net_income': float(data['净利润'].iloc[-1]),
            'eps': float(data['每股收益'].iloc[-1]),
            'report_date': data.index[-1],
            'currency': 'CNY'
        }

    def _process_hk_financials(self, data) -> Dict:
        """處理港股財務數據"""
        return {
            'revenue': float(data['营业收入'].iloc[-1]),
            'net_income': float(data['净利润'].iloc[-1]),
            'eps': float(data['每股收益'].iloc[-1]),
            'report_date': data.index[-1],
            'currency': 'HKD'
        }

    def _process_us_financials(self, data) -> Dict:
        """處理美股財務數據"""
        return {
            'revenue': float(data['总营收'].iloc[-1]),
            'net_income': float(data['净利润'].iloc[-1]),
            'eps': float(data['每股收益'].iloc[-1]),
            'report_date': data.index[-1],
            'currency': 'USD'
        }

    def _process_jp_financials(self, data) -> Dict:
        """處理日股財務數據"""
        return {
            'revenue': float(data['营业收入'].iloc[-1]),
            'net_income': float(data['净利润'].iloc[-1]),
            'eps': float(data['每股收益'].iloc[-1]),
            'report_date': data.index[-1],
            'currency': 'JPY'
        }