# data/fetchers/ibkr_fetcher.py
import asyncio
import logging
from config.settings import Settings
from ib_insync import IB
from typing import Dict, Optional, Any
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from .base_fetcher import BaseFetcher

settings = Settings.get_instance()

class IBKRConnectionError(Exception):
    """IBKR连接问题的自定义异常"""
    pass

class IBKRDataError(Exception):
    """IBKR数据获取异常"""
    pass

class IBKRFetcher(BaseFetcher):
    def __init__(self):
        self.ib = IB()
        self.connected = False
        self.logger = logging.getLogger(__name__)
        self.settings = Settings.get_instance()
        
        # 获取配置
        self.host = self.settings.ibkr.host
        self.port = self.settings.ibkr.port
        self.client_id = self.settings.ibkr.client_id
        self.timeout = self.settings.ibkr.timeout
        self.read_only = self.settings.ibkr.read_only

    async def is_available(self) -> bool:
        """检查IBKR连接是否可用"""
        try:
            if not self.connected:
                await self.connect()
            return self.ib.isConnected()
        except Exception as e:
            self.logger.warning(f"IBKR连接检查失败: {str(e)}")
            return False

    async def connect(self, retries=3, backoff=2):
        """实现带权限验证的智能重连机制"""
        attempt = 0
        while attempt < retries and not self.connected:
            try:
                # 建立连接
                await self.ib.connectAsync(
                    host=self.host,
                    port=self.port,
                    clientId=self.client_id,
                    timeout=self.timeout,
                    readonly=self.read_only  # 传递只读模式参数
                )
                self.connected = True
                self.logger.info(f"成功连接到IBKR Gateway（{'只读模式' if self.read_only else '交易模式'}）")
                
                # 验证模式一致性
                if not self.read_only:
                    await self._validate_trading_permission()
                    
                return True
            except ConnectionRefusedError as e:
                attempt += 1
                wait = backoff ** attempt
                self.logger.warning(f"连接尝试 {attempt} 失败，{wait}秒后重试...")
                await asyncio.sleep(wait)
            except Exception as e:
                self.logger.error(f"意外连接错误: {str(e)}")
                raise IBKRConnectionError(f"{retries}次尝试后连接失败") from e
        return False

    async def _validate_trading_permission(self):
        """验证交易权限（仅在非只读模式调用）"""
        try:
            # 尝试获取需要交易权限的账户信息
            accounts = await self.ib.reqManagedAcctsAsync()
            if not accounts:
                raise IBKRConnectionError("未找到有效交易账户")
                
            self.logger.info("交易权限验证通过")
            return True
        except Exception as e:
            if "Read-only API" in str(e):
                self.logger.error("IBKR Gateway处于只读模式，但配置要求交易权限")
                raise IBKRConnectionError("配置冲突：要求交易权限但网关为只读模式") from e
            raise

    async def fetch_market_data(self, symbol: str, country: str) -> Dict:
        """获取市场数据（带只读模式检查）"""
        if not self.connected:
            await self.connect()
            
        if self.read_only:
            self.logger.debug("只读模式下获取市场数据")
            
        contract = self._resolve_market(symbol, country)
        
        try:
            # 获取实时报价
            tick = await self.ib.reqMktDataAsync(contract)
            await asyncio.sleep(1)  # 等待数据稳定
            
            # 获取历史PE比率
            pe_ratio = await self.ib.reqFundamentalDataAsync(
                contract, 'ReportsFinSummary'
            )
            
            return {
                'symbol': symbol,
                'price': tick.last if tick else None,
                'pe': self._parse_pe_from_xml(pe_ratio),
                'volume': tick.volume if tick else None,
                'currency': contract.currency
            }
        except Exception as e:
            self.logger.error(f"市场数据获取失败: {str(e)}")
            raise

    async def place_order(self, order):
        """示例交易方法（带只读检查）"""
        if self.read_only:
            self.logger.error("禁止在只读模式下执行交易操作")
            raise PermissionError("只读模式下交易功能被禁用")
            
        # 实际下单逻辑
        return await self.ib.placeOrderAsync(order)

    async def fetch_financials(self, symbol: str, country: str) -> Dict:
        """获取完整财务报表（含重试逻辑）"""
        contract = self._resolve_market(symbol, country)
        
        try:
            # 获取年度报告（日本企业需要特殊处理）
            report_type = 'ReportsFinStatements' if country == 'JP' else 'ReportsFinSummary'
            raw_data = await self.ib.reqFundamentalDataAsync(contract, report_type)
            
            return self._parse_financial_xml(raw_data, country)
        except Exception as e:
            self.logger.error(f"Financial data fetch failed: {str(e)}")
            raise

    def _parse_pe_from_xml(self, xml_data: str) -> Optional[float]:
        """从XML数据中解析PE比率"""
        try:
            root = ET.fromstring(xml_data)
            return float(root.find('.//PeRatio').text)
        except (AttributeError, ValueError):
            return None

    def _parse_financial_xml(self, xml_data: str, country: str) -> Dict:
        """解析不同国家的财务报表XML"""
        root = ET.fromstring(xml_data)
        result = {'currency': 'USD'}
        
        # 通用字段解析
        result['revenue'] = self._get_xml_value(root, 'TotalRevenue')
        result['net_income'] = self._get_xml_value(root, 'NetIncome')
        
        # 日本市场特殊字段
        if country == 'JP':
            result['operating_profit'] = self._get_xml_value(root, 'OperatingIncome')
            result['equity_ratio'] = self._get_xml_value(root, 'EquityToAssetRatio')
            result['currency'] = 'JPY'
            
        return result

    def _get_xml_value(self, root, tag_name: str) -> Optional[float]:
        """安全解析XML数值"""
        elem = root.find(f'.//{tag_name}')
        return float(elem.text) if elem is not None and elem.text else None

    async def safe_shutdown(self):
        """安全关闭连接"""
        if self.connected:
            self.ib.disconnect()
            self.connected = False
            self.logger.info("已断开与IBKR Gateway的连接")

    async def close(self):
        """关闭IBKR连接"""
        try:
            if self.ib and self.ib.isConnected():
                self.logger.debug("正在断开IBKR连接...")
                await self.ib.disconnectAsync()
                self.connected = False
        except Exception as e:
            self.logger.error(f"断开IBKR连接时出错: {str(e)}")
            raise

    async def _fetch_raw_market_data(self, symbol: str, country: str) -> Dict[str, Any]:
        """获取原始市场数据"""
        try:
            if not self.ib.isConnected():
                await self.connect()
                
            # 创建合约对象
            contract = Stock(symbol, 'SMART', currency='USD')
            await self.ib.qualifyContractsAsync(contract)
            
            # 获取实时行情
            tickers = await self.ib.reqTickersAsync(contract)
            if not tickers:
                raise Exception(f"无法获取 {symbol} 的市场数据")
                
            ticker = tickers[0]
            return {
                'price': ticker.marketPrice(),
                'volume': ticker.volume,
                'currency': contract.currency,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"IBKR获取市场数据失败: {str(e)}")
            raise

    async def _fetch_raw_financials(self, symbol: str, country: str) -> Dict[str, Any]:
        """获取原始财务数据"""
        try:
            if not self.ib.isConnected():
                await self.connect()
                
            # 创建合约对象
            contract = Stock(symbol, 'SMART', currency='USD')
            await self.ib.qualifyContractsAsync(contract)
            
            # 获取基本面数据
            fundamentals = await self.ib.reqFundamentalDataAsync(
                contract, 
                reportType='ReportsFinSummary'
            )
            
            if not fundamentals:
                raise Exception(f"无法获取 {symbol} 的财务数据")
            
            # 解析XML格式的财务数据
            # TODO: 实现XML解析逻辑
            return {
                'pe_ratio': 0.0,
                'market_cap': 0.0,
                'eps': 0.0,
                'revenue': 0.0,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"IBKR获取财务数据失败: {str(e)}")
            raise


async def main():
    """验证只读模式的示例"""
    fetcher = IBKRFetcher()
    await fetcher.connect()
    
    try:
        # 测试数据获取
        data = await fetcher.fetch_market_data('AAPL', 'US')
        print(f"市场数据: {data}")
        
        # 测试交易操作（会触发错误）
        if not fetcher.read_only:
            from ib_insync import MarketOrder
            order = MarketOrder('BUY', 1)
            await fetcher.place_order(order)
        else:
            print("当前为只读模式，跳过交易测试")
            
    finally:
        await fetcher.safe_shutdown()

if __name__ == "__main__":
    # 根据环境更新日志级别
    if settings.env_state == "development":
            logging.getLogger().setLevel(logging.DEBUG)
    asyncio.run(main())