# data/fetchers/fmp_fetcher.py

import aiohttp
import logging
from typing import Dict, Any
from .base_fetcher import BaseFetcher
from config import get_settings
from config.settings import Settings
from tenacity import retry, stop_after_attempt, wait_exponential

settings = get_settings()

class FMPFetcher(BaseFetcher):
    """
    Financial Modeling Prep API数据获取器
    
    用于从FMP获取股票市场和财务数据的客户端。
    支持异步请求，包含错误处理和重试机制。
    
    Attributes:
        api_key: FMP API访问令牌
        base_url: API基础URL
        timeout: 请求超时时间
        period: 财务数据周期(annual/quarter)
    """
    def __init__(self):
        """
        初始化FMP数据获取器
        
        从全局配置中读取API密钥和其他设置
        设置日志记录器和基本参数
        """
        super().__init__('fmp')
        self.logger = logging.getLogger(__name__)
        self.settings = Settings.get_instance()
        self.api_key = settings.fmp.api_key
        self.base_url = "https://financialmodelingprep.com/api/v3/"
        self.timeout = settings.fmp.timeout
        self.period = settings.fmp.period

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    
    async def _make_request(self, session: aiohttp.ClientSession, url: str, params: Dict) -> Dict:
        """
        发送请求并处理响应
        
        Args:
            session: aiohttp会话实例
            url: 请求URL
            params: 请求参数字典
            
        Returns:
            Dict: API响应数据
            
        Raises:
            Exception: 当API返回错误或响应无效时
            aiohttp.ClientError: 网络请求错误
            ValueError: JSON解析错误
        """
        try:
            
            from urllib.parse import urlencode
            masked_params = {k: (v if k != "apikey" else "HIDE") for k, v in params.items()}
            full_url = f"{url}?{urlencode(masked_params)}"
            self.logger.debug(f"完整请求 URL: {full_url}")
            
            
            async with session.get(url, params=params) as response:
                if response.status == 403:
                    self.logger.error(f"FMP API 认证失败: API密钥可能无效或已过期")
                    raise Exception("FMP API认证失败，请检查API密钥")
                elif response.status != 200:
                    self.logger.error(f"FMP API请求失败: {response.status}")
                    error_text = await response.text()
                    self.logger.error(f"错误详情: {error_text}")
                    raise Exception(f"FMP API请求失败: {response.status}")
                
                data = await response.json()
                
                if not data:
                    raise Exception("API返回空数据")
                
                if isinstance(data, dict) and "Error Message" in data:
                    raise Exception(f"API返回错误: {data['Error Message']}")
                
                if isinstance(data, list) and len(data) == 0:
                    self.logger.warning(f"API返回空列表，可能没有找到数据: {url}")
                    return []
                
                self.logger.debug(f"成功获取数据，响应长度: {len(str(data))}")
                return data
                    
        except aiohttp.ClientError as e:
            self.logger.error(f"网络请求错误: {str(e)}")
            raise
        except ValueError as e:
            self.logger.error(f"JSON解析错误: {str(e)}")
            raise Exception(f"无效的API响应格式: {str(e)}")

    async def fetch_market_data(self, symbol: str) -> Dict[str, Any]:
        """
        获取股票市场数据
        
        获取实时价格和TTM市盈率等市场数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            Dict[str, Any]: {
                'price': float,  # 当前股价
                'pe_ratio': float  # 市盈率
            }
            
        Raises:
            Exception: 获取或处理数据失败时
        """
        params = {"apikey": self.api_key}
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                # 获取实时报价
                price_url = f"{self.base_url}stock/full/real-time-price/{symbol}"
                price_data = await self._make_request(session, price_url, params)
                
                # 获取估值比率
                ratio_url = f"{self.base_url}ratios-ttm/{symbol}"
                ratio_data = await self._make_request(session, ratio_url, params)
                
                latest_price_data = price_data[0] if price_data else {}
                lastest_ratio_data = ratio_data[0] if ratio_data else {}
            
                result = {
                    'price': float(latest_price_data.get('fmpLast', 0)),
                    'pe_ratio': float(lastest_ratio_data.get('priceEarningsRatioTTM', 0))
                }

                self.logger.debug(f"市场数据返回值: {result}")
                return result
            
        except Exception as e:
            self.logger.error(f"获取市场数据失败: {str(e)}")
            return None

    async def fetch_financials(self, symbol: str) -> Dict[str, Any]:
        """
        获取公司财务数据
        
        获取现金流量表、利润表和公司概况等财务数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            Dict[str, Any]: {
                'market_cap': float,  # 市值
                'currency': str,  # 货币单位
                'eps': float,  # 每股收益
                'shares_outstanding': float,  # 流通股数
                'revenue': float,  # 营收
                'free_cash_flow': float,  # 自由现金流
                'net_income': float  # 净利润
                ‘date’: datetime  # 财报日期
            }
            
        Raises:
            Exception: 获取或处理数据失败时
        """
        params = {"apikey": self.api_key}
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                # 获取现金流数据
                cash_flow_url = f"{self.base_url}cash-flow-statement/{symbol}"
                cash_flow_data = await self._make_request(session, cash_flow_url, {"period": self.period, **params})

                # 获取利润表数据
                income_url = f"{self.base_url}income-statement/{symbol}"
                income_data = await self._make_request(session, income_url, {"period": self.period, **params})
                
                # 获取公司概况数据
                profile_url = f"{self.base_url}profile/{symbol}"
                profile_data = await self._make_request(session, profile_url, params)

                # 转换为统一格式
                latest_cash_flow = cash_flow_data[0] if cash_flow_data else {}
                latest_income = income_data[0] if income_data else {}
                profile = profile_data[0] if profile_data else {}

                # 计算PE
                current_price = float(profile.get('price', 0))

                # 计算流通股数
                market_cap = float(profile.get('mktCap', 0))
                shares_outstanding = market_cap / current_price if current_price and current_price != 0 else 0
                
                result = {
                    'market_cap': market_cap,
                    'currency': str(latest_income.get('reportedCurrency', 0)),
                    'eps': float(latest_income.get('eps', 0)),
                    'shares_outstanding': shares_outstanding,
                    'revenue': float(latest_income.get('revenue', 0)),
                    'free_cash_flow': float(latest_cash_flow.get('freeCashFlow', 0)),
                    'net_income': float(latest_income.get('netIncome', 0)),
                    'date': latest_income.get('date', '')
                }

                self.logger.debug(f"财务数据返回值: {result}")
                return result

        except Exception as e:
            self.logger.error(f"获取财务数据失败: {str(e)}")
            raise

    async def is_available(self) -> bool:
        """
        检查数据源是否可用
        
        验证是否配置了有效的API密钥
        
        Returns:
            bool: 如果API密钥存在则返回True，否则False
        """
        return bool(self.api_key) #有key就返回True