# data/fetchers/yahoo_fetcher.py
import aiohttp
from typing import Dict, Optional, Any
from abc import ABC, abstractmethod
from .base_fetcher import BaseFetcher
from config import get_settings
import asyncio
import yfinance as yf
from datetime import datetime

settings = get_settings()

class YahooFetcher(BaseFetcher, ABC):
    """Yahoo Finance数据获取器"""
    
    def __init__(self):
        super().__init__('yahoo')
        self.base_url = "https://query1.finance.yahoo.com"
        self.timeout = settings.yahoo.timeout

    async def _fetch_raw_market_data(self, symbol: str, country: str) -> Dict[str, Any]:
        """使用 yfinance 获取市场数据"""
        loop = asyncio.get_event_loop()
        ticker = await loop.run_in_executor(None, lambda: yf.Ticker(symbol))
        info = ticker.info
        return {
            'price': info.get('regularMarketPrice'),
            'volume': info.get('volume'),
            'currency': info.get('currency'),
            'timestamp': datetime.now().isoformat()
        }

    async def _fetch_raw_financials(self, symbol: str, country: str) -> Dict[str, Any]:
        """使用 yfinance 获取财务数据"""
        loop = asyncio.get_event_loop()
        ticker = await loop.run_in_executor(None, lambda: yf.Ticker(symbol))
        info = ticker.info
        return {
            'pe_ratio': info.get('trailingPE', 0),
            'market_cap': info.get('marketCap', 0),
            'eps': info.get('epsTrailingSevenDays', 0),
            'revenue': info.get('regularMarketPreviousClose', 0),  # 用作示例
            'timestamp': datetime.now().isoformat()
        }

    async def fetch_market_data(self, symbol: str, country: str) -> Dict[str, Any]:
        return await self._fetch_raw_market_data(symbol, country)

    async def fetch_financials(self, symbol: str, country: str) -> Dict[str, Any]:
        return await self._fetch_raw_financials(symbol, country)

    async def is_available(self) -> bool:
        """简单地认为 Yahoo 连接始终可用"""
        return True

    async def close(self):
        """YahooFetcher 无需特殊清理，可留空"""
        pass
