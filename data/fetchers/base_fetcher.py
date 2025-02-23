# data/fetchers/base_fetcher.py
import abc
import logging
import asyncio
from typing import Dict, Optional, Type, Any
from datetime import datetime, timedelta
from functools import wraps
from dataclasses import dataclass
from config import get_settings
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)
settings = get_settings()

class DataFetchError(Exception):
    """数据获取异常基类"""
    pass

class RetryExhaustedError(DataFetchError):
    """重试耗尽异常"""
    pass

@dataclass
class HealthStatus:
    """数据源健康状态记录"""
    success_count: int = 0
    error_count: int = 0
    last_success: Optional[datetime] = None
    last_error: Optional[datetime] = None

    @property
    def is_healthy(self) -> bool:
        """健康状态判定"""
        if self.error_count == 0:
            return True
        return (self.success_count / (self.success_count + self.error_count)) > 0.8

class BaseFetcherMeta(abc.ABCMeta):
    """元类用于自动注册子类"""
    _registry = {}

    def __new__(cls, name, bases, namespace):
        new_cls = super().__new__(cls, name, bases, namespace)
        if name != 'BaseFetcher':
            cls._registry[name.lower()] = new_cls
        return new_cls

def retry(max_retries=3, backoff_factor=0.5):
    """带指数退避的重试装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            retries = 0
            while retries <= max_retries:
                try:
                    return await func(self, *args, **kwargs)
                except DataFetchError as e:
                    if retries == max_retries:
                        logger.error(f"重试耗尽 ({max_retries}次) @ {self.source_name}")
                        raise RetryExhaustedError from e
                    
                    wait_time = backoff_factor * (2 ** retries)
                    logger.warning(f"重试 {retries+1}/{max_retries} @ {self.source_name} - {str(e)}")
                    await asyncio.sleep(wait_time)
                    retries += 1
            raise RetryExhaustedError
        return wrapper
    return decorator

class BaseFetcher(metaclass=BaseFetcherMeta):
    """财务数据获取器抽象基类"""
    
    priority: int = 0

    # 类级健康状态跟踪
    _health_status: Dict[str, HealthStatus] = {}

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.priority = settings.data_sources.priority.index(source_name)
        self._health_status.setdefault(source_name, HealthStatus())
        self._cache = {}
        self.timeout = getattr(settings, source_name).timeout

    @classmethod
    def get_available_fetchers(cls) -> Dict[str, Type['BaseFetcher']]:
        """获取所有已注册的数据获取器类"""
        return cls._registry

    @abc.abstractmethod
    async def is_available(self) -> bool:
        """检查数据源是否可用"""
        pass

    @abc.abstractmethod
    async def _fetch_raw_market_data(self, symbol: str, country: str) -> Dict:
        """原始市场数据获取方法（需子类实现）"""
        pass

    @abc.abstractmethod
    async def _fetch_raw_financials(self, symbol: str, country: str) -> Dict:
        """原始财务数据获取方法（需子类实现）"""
        pass

    @retry(max_retries=3)
    async def fetch_market_data(self, symbol: str, country: str) -> Optional[Dict]:
        """带重试机制的市场数据获取"""
        cache_key = f"market_{symbol}_{country}"
        if cache_data := self._get_cache(cache_key):
            return cache_data

        try:
            raw_data = await self._fetch_raw_market_data(symbol, country)
            validated = self._validate_market_data(raw_data)
            normalized = self._normalize_market_data(validated)
            self._update_health(True)
            self._set_cache(cache_key, normalized)
            return normalized
        except Exception as e:
            self._update_health(False)
            raise DataFetchError(f"{self.source_name}市场数据获取失败") from e

    @retry(max_retries=3)
    async def fetch_financials(self, symbol: str, country: str) -> Optional[Dict]:
        """带重试机制的财务数据获取"""
        cache_key = f"financials_{symbol}_{country}"
        if cache_data := self._get_cache(cache_key):
            return cache_data

        try:
            raw_data = await self._fetch_raw_financials(symbol, country)
            validated = self._validate_financials(raw_data)
            normalized = self._normalize_financials(validated)
            self._update_health(True)
            self._set_cache(cache_key, normalized)
            return normalized
        except Exception as e:
            self._update_health(False)
            raise DataFetchError(f"{self.source_name}财务数据获取失败") from e

    def _validate_market_data(self, data: Dict) -> Dict:
        """市场数据验证"""
        if not isinstance(data, dict):
            raise ValidationError("市场数据格式无效")
        if 'price' not in data or not isinstance(data['price'], (int, float)):
            raise ValidationError("缺少有效的价格数据")
        return data

    def _validate_financials(self, data: Dict) -> Dict:
        """财务数据验证"""
        required_fields = ['revenue', 'net_income']
        for field in required_fields:
            if field not in data or not isinstance(data[field], (int, float)):
                raise ValidationError(f"缺少有效的{field}数据")
        return data

    def _normalize_market_data(self, data: Dict) -> Dict:
        """市场数据标准化"""
        return {
            'source': self.source_name,
            'price': float(data.get('price', 0)),
            'volume': int(data.get('volume', 0)),
            'pe_ratio': float(data.get('pe_ratio', 0)) if data.get('pe_ratio') else None,
            'currency': data.get('currency', 'USD'),
            'timestamp': datetime.now().isoformat()
        }

    def _normalize_financials(self, data: Dict) -> Dict:
        """财务数据标准化"""
        return {
            'source': self.source_name,
            'revenue': float(data.get('revenue', 0)),
            'net_income': float(data.get('net_income', 0)),
            'eps': float(data.get('eps', 0)) if data.get('eps') else None,
            'report_date': data.get('report_date', datetime.now().date().isoformat()),
            'currency': data.get('currency', 'USD')
        }

    def _update_health(self, success: bool):
        """更新健康状态"""
        status = self._health_status[self.source_name]
        if success:
            status.success_count += 1
            status.last_success = datetime.now()
        else:
            status.error_count += 1
            status.last_error = datetime.now()

    @property
    def health_status(self) -> HealthStatus:
        """获取当前健康状态"""
        return self._health_status[self.source_name]

    @property
    def is_healthy(self) -> bool:
        """健康状态判定"""
        return self.health_status.is_healthy

    @property
    def is_available(self) -> bool:
        """数据源是否可用"""
        return self.source_name in settings.data_sources.priority and self.is_healthy

    def _get_cache(self, key: str) -> Optional[Dict]:
        """获取缓存数据"""
        entry = self._cache.get(key)
        if entry and datetime.now() < entry['expire']:
            return entry['data']
        return None

    def _set_cache(self, key: str, data: Dict, ttl: int = 300):
        """设置缓存数据"""
        self._cache[key] = {
            'data': data,
            'expire': datetime.now() + timedelta(seconds=ttl)
        }

class MarketData(BaseModel):
    """标准化市场数据模型"""
    source: str
    price: float
    volume: int
    pe_ratio: Optional[float]
    currency: str
    timestamp: str

class FinancialData(BaseModel):
    """标准化财务数据模型"""
    source: str
    revenue: float
    net_income: float
    eps: Optional[float]
    report_date: str
    currency: str
