# data/fetchers/data_service.py
import logging
from typing import Dict, Any
import ssl
import aiohttp
import certifi
from config.settings import Settings
#from .ibkr_fetcher import IBKRFetcher
from .yahoo_fetcher import YahooFetcher
from .alpha_vantage_fetcher import AlphaVantageFetcher
from .fmp_fetcher import FMPFetcher

logger = logging.getLogger(__name__)

class DataService:
    """数据服务类，管理所有数据源"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.settings = Settings.get_instance()
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.session = None
        self.fetchers = {
            'yahoo': YahooFetcher(),
            'alpha_vantage': AlphaVantageFetcher(),
            'fmp': FMPFetcher(),
            #'ibkr': IBKRFetcher()
        }
        
    async def get_market_data(self, symbol: str, country: str) -> Dict[str, Any]:
        """获取市场数据，带故障转移机制"""
        errors = []
        
        # 按优先级尝试不同数据源
        for source_name in self.settings.data_sources.priority:
            fetcher = self.fetchers.get(source_name)
            if not fetcher:
                continue
                
            try:
                self.logger.debug(f"尝试从 {source_name} 获取市场数据")
                if await fetcher.is_available():
                    data = await fetcher.fetch_market_data(symbol, country)
                    self.logger.info(f"成功从 {source_name} 获取市场数据")
                    return data
            except Exception as e:
                self.logger.warning(f"{source_name} 数据获取失败: {str(e)}")
                errors.append(f"{source_name}: {str(e)}")
        
        raise Exception(f"所有数据源均不可用\n详细错误: {'; '.join(errors)}")

    async def get_financials(self, symbol: str, country: str) -> Dict[str, Any]:
        """获取财务数据，带故障转移机制（排除 alpha_vantage 获取财务信息）"""
        errors = []

        # 使用优先级配置，但排除 alpha_vantage 数据获取器
        for source_name in self.settings.data_sources.priority:
            # 排除 alpha_vantage，因为它只支持市场报价信息
            if source_name == 'alpha_vantage':
                self.logger.debug("跳过 alpha_vantage 获取财务数据")
                continue

            fetcher = self.fetchers.get(source_name)
            if not fetcher:
                continue

            try:
                self.logger.debug(f"尝试从 {source_name} 获取财务数据")
                if await fetcher.is_available():
                    data = await fetcher.fetch_financials(symbol, country)
                    self.logger.info(f"成功从 {source_name} 获取财务数据")
                    return data
            except Exception as e:
                self.logger.exception(f"错误发生于 {source_name} 获取财务数据")
                errors.append(f"{source_name}: {str(e)}")
    
        raise Exception(f"无法获取财务数据\n详细错误: {'; '.join(errors)}")

    async def close(self):
        """关闭所有资源"""
        try:
            if self.session:
                await self.session.close()
            for fetcher in self.fetchers.values():
                if hasattr(fetcher, 'close'):
                    await fetcher.close()
        except Exception as e:
            self.logger.error(f"关闭数据服务时出错: {str(e)}")
            raise

    def _normalize_data(self, data: Dict, source: str) -> Dict:
        """标准化不同数据源的字段"""
        mapping = {
            'yahoo': {
                'price': 'regularMarketPrice',
                'pe': 'trailingPE',
                'volume': 'regularMarketVolume'
            },
            'alpha_vantage': {
                'price': 'price',
                'volume': 'volume'
            }
        }
        
        normalized = {}
        for target_field, source_field in mapping.get(source, {}).items():
            normalized[target_field] = data.get(source_field)
            
        return normalized
    
    async def _create_session(self):
        if not self.session:
            connector = aiohttp.TCPConnector(ssl=self.ssl_context)
            self.session = aiohttp.ClientSession(connector=connector)
        return self.session