# data/fetchers/fmp_fetcher.py
import aiohttp
import asyncio
import logging
from typing import Dict, Optional, Any
from .base_fetcher import BaseFetcher
from config import get_settings
from config.settings import Settings
from datetime import datetime

settings = get_settings()

class FMPFetcher(BaseFetcher):
    """Financial Modeling Prep API数据获取器"""
    
    def __init__(self):
        super().__init__('fmp')
        self.logger = logging.getLogger(__name__)
        self.settings = Settings.get_instance()
        self.api_key = self.settings.fmp.api_key
        self.base_url = "https://financialmodelingprep.com/api/v3"
        self.timeout = settings.fmp.timeout

    async def fetch_market_data(self, symbol: str, country: str) -> Optional[Dict]:
        params = {
            'apikey': self.api_key
        }
        
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                # 获取实时报价
                quote_url = f"{self.base_url}/quote/{self._format_symbol(symbol, country)}"
                async with session.get(quote_url, params=params) as resp:
                    quote_data = await resp.json()
                    
                # 获取估值比率
                ratio_url = f"{self.base_url}/ratios-ttm/{self._format_symbol(symbol, country)}"
                async with session.get(ratio_url, params=params) as resp:
                    ratio_data = await resp.json()
                    
                return self._parse_response(quote_data, ratio_data)
        except Exception as e:
            return None

    async def fetch_financials(self, symbol: str, country: str) -> Dict[str, Any]:
        """从FMP获取财务数据"""
        try:
            # 构建API请求URL
            url = f"{self.base_url}/profile/{symbol}"
            params = {"apikey": self.api_key}

            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if (response.status != 200):
                        raise Exception(f"FMP API请求失败: {response.status}")
                    
                    data = await response.json()
                    
                    if not data or isinstance(data, dict) and "Error Message" in data:
                        raise Exception(f"FMP API错误: {data.get('Error Message', '未知错误')}")
                    
                    # 转换为统一格式
                    company_data = data[0] if isinstance(data, list) and data else {}
                    financials = {
                        "pe_ratio": float(company_data.get("pe", 0)),
                        "market_cap": float(company_data.get("mktCap", 0)),
                        "eps": float(company_data.get("eps", 0)),
                        "dividend_yield": float(company_data.get("dividend_yield", 0)),
                        "revenue": float(company_data.get("revenue", 0)),
                        "shares_outstanding": float(company_data.get("sharesOutstanding", 0))
                    }
                    
                    return financials

        except Exception as e:
            self.logger.error(f"从FMP获取财务数据失败: {str(e)}")
            raise

    async def is_available(self) -> bool:
        return bool(self.api_key)  # 如果有API密钥就认为可用
    
    def _format_symbol(self, symbol: str, country: str) -> str:
        return f"{symbol}.{'T' if country == 'JP' else ''}{country.upper()}"

    def _parse_response(self, quote: list, ratios: list) -> Dict:
        try:
            return {
                'price': quote[0]['price'],
                'pe': ratios[0]['priceEarningsRatioTTM'],
                'volume': quote[0]['volume']
            }
        except (KeyError, IndexError, TypeError):
            return None

    async def _fetch_raw_market_data(self, symbol: str, country: str) -> Dict[str, Any]:
        """获取实时市场数据"""
        try:
            url = f"{self.base_url}/quote/{symbol}"
            params = {"apikey": self.api_key}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        raise Exception(f"FMP API请求失败: {response.status}")
                    
                    data = await response.json()
                    if not data:
                        raise Exception(f"未找到股票 {symbol} 的市场数据")
                    
                    quote = data[0]
                    return {
                        'price': float(quote.get('price', 0)),
                        'volume': int(quote.get('volume', 0)),
                        'currency': quote.get('currency', 'USD'),
                        'timestamp': datetime.now().isoformat()
                    }
                    
        except Exception as e:
            self.logger.error(f"获取市场数据失败: {str(e)}")
            raise

    async def _fetch_raw_financials(self, symbol: str, country: str) -> Dict[str, Any]:
        """获取财务数据"""
        try:
            # 获取现金流数据
            cash_flow_url = f"{self.base_url}/cash-flow-statement/{symbol}"
            income_url = f"{self.base_url}/income-statement/{symbol}"
            profile_url = f"{self.base_url}/profile/{symbol}"
            
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.ssl_context)) as session:
                # 获取现金流数据
                async with session.get(cash_flow_url, params={"apikey": self.api_key, "limit": 1}) as response:
                    if response.status != 200:
                        raise Exception(f"FMP API请求失败: {response.status}")
                    cash_flow_data = await response.json()
                
                # 获取利润表数据
                async with session.get(income_url, params={"apikey": self.api_key, "limit": 1}) as response:
                    if response.status != 200:
                        raise Exception(f"FMP API请求失败: {response.status}")
                    income_data = await response.json()
                
                # 获取公司概况数据
                async with session.get(profile_url, params={"apikey": self.api_key}) as response:
                    if response.status != 200:
                        raise Exception(f"FMP API请求失败: {response.status}")
                    profile_data = await response.json()

            # 提取最新的财务数据
            latest_cash_flow = cash_flow_data[0] if cash_flow_data else {}
            latest_income = income_data[0] if income_data else {}
            profile = profile_data[0] if profile_data else {}

            # 计算自由现金流
            operating_cash_flow = float(latest_cash_flow.get('operatingCashFlow', 0))
            capital_expenditure = float(latest_cash_flow.get('capitalExpenditure', 0))
            free_cash_flow = operating_cash_flow - capital_expenditure

            return {
                'pe_ratio': float(profile.get('pe', 0)),
                'market_cap': float(profile.get('mktCap', 0)),
                'eps': float(latest_income.get('eps', 0)),
                'revenue': float(latest_income.get('revenue', 0)),
                'free_cash_flow': free_cash_flow,  # 添加自由现金流
                'shares_outstanding': float(profile.get('sharesOutstanding', 0))
            }

        except Exception as e:
            self.logger.error(f"从FMP获取财务数据失败: {str(e)}")
            raise
