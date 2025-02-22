# data/fetchers/financial_parser.py
import logging
import xml.etree.ElementTree as ET
from typing import Dict, Optional, Tuple
from datetime import datetime
from decimal import Decimal, InvalidOperation
import requests
from config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

class FinancialParser:
    """智能财务报告解析系统（支持美日会计准则）"""
    
    def __init__(self):
        self.currency_rates = {'JPY/USD': None}
        self._init_currency_conversion()

    def _init_currency_conversion(self):
        """初始化货币汇率（带缓存机制）"""
        try:
            response = requests.get(
                f"https://api.exchangerate-api.com/v6/latest?base=JPY",
                timeout=5
            )
            self.currency_rates['JPY/USD'] = response.json()['rates']['USD']
            logger.info("Currency rates updated successfully")
        except Exception as e:
            logger.warning(f"Failed to update currency rates: {str(e)}")

    def parse_report(self, raw_xml: str, country: str) -> Dict:
        """智能路由解析器（根据国家选择处理方式）"""
        root = ET.fromstring(raw_xml)
        
        if country.upper() == 'US':
            return self._parse_us_gaap(root)
        elif country.upper() == 'JP':
            return self._parse_jp_ifrs(root)
        else:
            raise ValueError(f"Unsupported country code: {country}")

    def _parse_us_gaap(self, root: ET.Element) -> Dict:
        """解析美国GAAP标准报告"""
        period = self._detect_reporting_period(root)
        
        return {
            'period': period,
            'revenue': self._extract_value(root, 'TotalRevenue'),
            'net_income': self._extract_value(root, 'NetIncome'),
            'eps': self._extract_value(root, 'EarningsPerShareBasic'),
            'assets': self._extract_value(root, 'TotalAssets'),
            'liabilities': self._extract_value(root, 'TotalLiabilities'),
            'equity': self._extract_value(root, 'TotalEquity'),
            'currency': 'USD',
            'reporting_standard': 'US-GAAP'
        }

    def _parse_jp_ifrs(self, root: ET.Element) -> Dict:
        """解析日本IFRS标准报告（含货币转换）"""
        period = self._detect_reporting_period(root)
        raw_data = {
            'period': period,
            'revenue': self._extract_value(root, 'NetSales'),
            'operating_income': self._extract_value(root, 'OperatingIncome'),
            'net_income': self._extract_value(root, 'ProfitAttributableToOwners'),
            'equity_ratio': self._extract_ratio(root, 'EquityToAssetRatio'),
            'assets': self._extract_value(root, 'TotalAssets'),
            'liabilities': self._extract_value(root, 'TotalLiabilities'),
            'currency': 'JPY',
            'reporting_standard': 'JP-IFRS'
        }
        
        # 自动转换为美元计价
        return self._convert_jpy_to_usd(raw_data)

    def _convert_jpy_to_usd(self, data: Dict) -> Dict:
        """日元转美元换算系统"""
        if not self.currency_rates['JPY/USD']:
            logger.error("Currency rate not available, conversion skipped")
            return data
            
        converted = data.copy()
        rate = Decimal(self.currency_rates['JPY/USD'])
        
        for key in ['revenue', 'operating_income', 'net_income', 'assets', 'liabilities']:
            if data.get(key) is not None:
                try:
                    converted[f"{key}_usd"] = str(Decimal(data[key]) * rate)
                except InvalidOperation:
                    logger.warning(f"Invalid value for conversion: {data[key]}")
                    
        converted['conversion_rate'] = str(rate)
        converted['conversion_date'] = datetime.now().isoformat()
        return converted

    def _detect_reporting_period(self, root: ET.Element) -> Tuple[str, str]:
        """智能检测财报期间"""
        period_start = root.find('.//PeriodStartDate')
        period_end = root.find('.//PeriodEndDate')
        
        if period_start is not None and period_end is not None:
            return (period_start.text, period_end.text)
            
        # 日本报告的特殊处理
        fiscal_year = root.find('.//FiscalYear')
        if fiscal_year is not None:
            year = int(fiscal_year.text)
            return (f"{year}-04-01", f"{year+1}-03-31")
            
        return (None, None)

    def _extract_value(self, root: ET.Element, tag: str) -> Optional[str]:
        """安全提取数值字段"""
        elem = root.find(f'.//{tag}')
        if elem is not None and elem.text:
            try:
                # 移除逗号并验证数值有效性
                return str(Decimal(elem.text.replace(',', '')))
            except InvalidOperation:
                logger.warning(f"Invalid numeric value in {tag}: {elem.text}")
        return None

    def _extract_ratio(self, root: ET.Element, tag: str) -> Optional[str]:
        """提取比率字段（百分比处理）"""
        elem = root.find(f'.//{tag}')
        if elem is not None and elem.text:
            try:
                # 将百分比转换为小数
                return str(Decimal(elem.text.rstrip('%')) / 100)
            except InvalidOperation:
                logger.warning(f"Invalid ratio value in {tag}: {elem.text}")
        return None

# 单元测试示例
if __name__ == "__main__":
    import xml.dom.minidom
    
    # 测试用美国财报XML
    us_xml = """
    <Report>
        <TotalRevenue>123,456,789</TotalRevenue>
        <NetIncome>45,678,900</NetIncome>
        <EarningsPerShareBasic>5.67</EarningsPerShareBasic>
        <TotalAssets>987654321</TotalAssets>
        <TotalLiabilities>555555555</TotalLiabilities>
        <TotalEquity>432,098,765</TotalEquity>
        <PeriodStartDate>2023-01-01</PeriodStartDate>
        <PeriodEndDate>2023-12-31</PeriodEndDate>
    </Report>
    """
    
    # 测试用日本财报XML 
    jp_xml = """
    <FinancialReport>
        <NetSales>12,345,678,901</NetSales>
        <OperatingIncome>1,234,567,890</OperatingIncome>
        <ProfitAttributableToOwners>987,654,321</ProfitAttributableToOwners>
        <EquityToAssetRatio>45.67%</EquityToAssetRatio>
        <TotalAssets>9,876,543,210</TotalAssets>
        <TotalLiabilities>5,555,555,555</TotalLiabilities>
        <FiscalYear>2023</FiscalYear>
    </FinancialReport>
    """
    
    parser = FinancialParser()
    
    print("Parsing US report:")
    pretty_us = xml.dom.minidom.parseString(us_xml).toprettyxml()
    print(pretty_us)
    print("Result:", parser.parse_report(us_xml, 'US'))
    
    print("\nParsing JP report:")
    pretty_jp = xml.dom.minidom.parseString(jp_xml).toprettyxml()
    print(pretty_jp)
    print("Result:", parser.parse_report(jp_xml, 'JP'))
