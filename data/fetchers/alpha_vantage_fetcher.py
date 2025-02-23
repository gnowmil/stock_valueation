# data/fetchers/alpha_vantage_fetcher.py
from typing import Dict, Optional, Any
import aiohttp
import logging
from datetime import datetime
from .base_fetcher import BaseFetcher
from config import get_settings
from config.settings import Settings

settings = get_settings()

class AlphaVantageFetcher(BaseFetcher):
    """Alpha Vantage数据获取器"""
    
    def __init__(self):
        super().__init__('alpha_vantage')
        self.logger = logging.getLogger(__name__)
        if settings.env_state == "development":
            logging.getLogger().setLevel(logging.DEBUG)
        self.settings = Settings.get_instance()
        self.api_key = self.settings.alpha_vantage.api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.timeout = settings.alpha_vantage.timeout

    async def fetch_market_data(self, symbol: str, country: str) -> Optional[Dict]:
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': self._format_symbol(symbol, country),
            'apikey': self.api_key
        }
        
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(self.base_url, params=params) as resp:
                    data = await resp.json()
                    return self._parse_av_response(data)
        except Exception:
            return None

    async def fetch_financials(self, symbol: str, country: str) -> Dict[str, Any]:
        """获取 Alpha Vantage 财务数据"""
        try:
            params = {
                "function": "OVERVIEW",
                "symbol": symbol,
                "apikey": self.api_key
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params) as response:
                    if response.status != 200:
                        raise Exception(f"Alpha Vantage API 请求失败: {response.status}")
                    
                    data = await response.json()
                    
                    if "Error Message" in data:
                        raise Exception(f"Alpha Vantage API 错误: {data['Error Message']}")
                    
                    # 转换为标准格式
                    financials = {
                        "pe_ratio": float(data.get("PERatio", 0)),
                        "market_cap": float(data.get("MarketCapitalization", 0)),
                        "eps": float(data.get("EPS", 0)),
                        "dividend_yield": float(data.get("DividendYield", 0)),
                        "revenue": float(data.get("Revenue", 0))
                    }
                    
                    return financials
                    
        except Exception as e:
            self.logger.error(f"从 Alpha Vantage 获取财务数据失败: {str(e)}")
            raise
        
    async def is_available(self) -> bool:
        return bool(self.api_key)  # 如果有API密钥就认为可用
    
    def _format_symbol(self, symbol: str, country: str) -> str:
        return f"{symbol}.{'T' if country == 'JP' else ''}{country.upper()}"
    
    def _parse_av_response(self, data: Dict) -> Dict:
        try:
            quote = data['Global Quote']
            return {
                'price': float(quote['05. price']),
                'volume': int(quote['06. volume']),
                'pe': None  # Alpha Vantage不直接提供PE
            }
        except (KeyError, ValueError):
            return None

    async def _fetch_raw_market_data(self, symbol: str, country: str) -> Dict[str, Any]:
        """从 Alpha Vantage 获取市场数据"""
        try:
            params = {
                "function": "GLOBAL_QUOTE",
                "symbol": symbol,
                "apikey": self.api_key
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params) as response:
                    if response.status != 200:
                        raise Exception(f"Alpha Vantage API请求失败: {response.status}")
                    
                    data = await response.json()
                    quote = data.get("Global Quote", {})
                    
                    return {
                        'price': float(quote.get("05. price", 0)),
                        'volume': int(quote.get("06. volume", 0)),
                        'currency': 'USD',  # Alpha Vantage 默认使用 USD
                        'timestamp': datetime.now().isoformat()
                    }
        except Exception as e:
            self.logger.error(f"获取市场数据失败: {str(e)}")
            raise

    async def _fetch_raw_financials(self, symbol: str, country: str) -> Dict[str, Any]:
        try:
            async with aiohttp.ClientSession() as session:
                # 获取现金流数据
                cash_flow_params = {
                    "function": "CASH_FLOW",
                    "symbol": symbol,
                    "apikey": self.api_key
                }

                async with session.get(self.base_url, params=cash_flow_params) as response:
                    cash_flow_data = await response.json()

                # 检查API错误响应
                if 'Error Message' in cash_flow_data:
                    raise Exception(f"现金流接口错误: {cash_flow_data['Error Message']}")

                # 安全处理annualReports数据
                annual_reports = cash_flow_data.get('annualReports', [])
                if not annual_reports:
                    self.logger.warning(f"无年度现金流报告数据，symbol={symbol}")
                    reports = {}
                else:
                    reports = annual_reports[0]  # 取最新年度报告

                # 提取正确的字段名
                try:
                    operating_cashflow = float(reports.get('operatingCashFlow', 0))  # 使用正确的字段名
                except (TypeError, ValueError) as e:
                    self.logger.warning(f"解析operatingCashFlow失败: {str(e)}, 使用默认值0")
                    operating_cashflow = 0

                try:
                    capital_expenditure = float(reports.get('capitalExpenditure', 0))  # 使用正确的字段名
                except (TypeError, ValueError) as e:
                    self.logger.warning(f"解析capitalExpenditure失败: {str(e)}, 使用默认值0")
                    capital_expenditure = 0

                free_cash_flow = operating_cashflow - capital_expenditure

                # 获取其他财务数据
                overview_params = {
                    "function": "OVERVIEW",
                    "symbol": symbol,
                    "apikey": self.api_key
                }

                async with session.get(self.base_url, params=overview_params) as response:
                    overview_data = await response.json()

                # 尝试从 overview 数据中获取经营现金流和资本支出，如果没有返回则设为0
                operating_cashflow = float(overview_data.get("OperatingCashflow", 0))
                capital_expenditures = float(overview_data.get("CapitalExpenditures", 0))
                free_cash_flow = operating_cashflow - capital_expenditures

                # 如果数据为空而 free_cash_flow 等于0，可以选择使用默认值或其他替代方案
                return {
                    'pe_ratio': float(overview_data.get('PERatio', 0)),
                    'market_cap': float(overview_data.get('MarketCapitalization', 0)),
                    'eps': float(overview_data.get('EPS', 0)),
                    'revenue': float(overview_data.get('RevenueTTM', 0)),
                    'free_cash_flow': free_cash_flow,  # 确保返回该键
                    'shares_outstanding': float(overview_data.get('SharesOutstanding', 0))
                }

        except Exception as e:
            self.logger.error(f"从 Alpha Vantage 获取财务数据失败: {str(e)}")
            raise

