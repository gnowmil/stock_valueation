# data/fetchers/data_service.py
import logging
from typing import Dict, Any
import ssl
import certifi
from config.settings import Settings
from .fmp_fetcher import FMPFetcher

#logger = logging.getLogger(__name__)

class DataService:
    """数据服务类"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.settings = Settings.get_instance()
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.session = None
        self.fetcher = FMPFetcher()

    async def get_market_data(self, symbol: str) -> Dict[str, Any]:
        """获取市场数据"""
        try:
            self.logger.debug(f"从 FMP 获取市场数据")
            if await self.fetcher.is_available():
                data = await self.fetcher.fetch_market_data(symbol)
                self.logger.info(f"成功获取市场数据")
                return data
        except Exception as e:
            self.logger.error(f"市场数据获取失败: {str(e)}")
            raise

    async def get_financials(self, symbol: str) -> Dict[str, Any]:
        """获取财务数据"""
        try:
            self.logger.debug(f"从 FMP 获取财务数据")
            if await self.fetcher.is_available():
                data = await self.fetcher.fetch_financials(symbol)
                self.logger.info(f"成功获取财务数据")
                return data
        except Exception as e:
            self.logger.error(f"财务数据获取失败: {str(e)}")
            raise

    async def close(self):
        """关闭资源"""
        try:
            if self.session:
                await self.session.close()
        except Exception as e:
            self.logger.error(f"关闭数据服务时出错: {str(e)}")