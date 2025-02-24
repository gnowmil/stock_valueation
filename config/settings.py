import logging
import yaml
from pathlib import Path
from enum import Enum
from typing import ClassVar, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationError
import re
from aiohttp import ClientTimeout

class EnvironmentState(str, Enum):
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"

class FMPConfig(BaseModel):
    """Financial Modeling Prep配置"""
    api_key: str = Field(..., description="API访问令牌")
    timeout_seconds: int = Field(
        default=15,
        description="API请求超时时间(秒)"
    )
    period: str = Field('annual', description="财务数据频率")

    @field_validator('period')
    def validate_period(cls, v: str) -> str:
        valid_periods = {'annual', 'quarter'}
        if v.lower() not in valid_periods:
            raise ValueError(
                f"无效的财务数据频率: {v}。"
                f"有效值为: {', '.join(valid_periods)}"
            )
        return v.lower()

    @property
    def timeout(self) -> ClientTimeout:
        """转换为 aiohttp.ClientTimeout 对象"""
        return ClientTimeout(total=self.timeout_seconds)

class TelegramConfig(BaseModel):
    """Telegram通知配置"""
    bot_token: str = Field(..., description="机器人API令牌")
    chat_id: int = Field(..., description="聊天ID")
    timeout_seconds: int = Field(default=30, description="超时时间(秒)", gt=0, le=300)

    @property
    def timeout(self) -> ClientTimeout:
        """转换为 aiohttp.ClientTimeout 对象"""
        return ClientTimeout(total=self.timeout_seconds)

class ModelConfig(BaseModel):
    """估值模型参数配置"""
    monte_carlo_sims: int = Field(10000, gt=0)
    risk_free_rate: float = Field(0.02, ge=0, le=1)
    pe_percentile: float = Field(0.8, ge=0, le=1)
    dcf_growth_years: int = Field(5, gt=0)

class LoggingConfig(BaseModel):
    """日志系统配置"""
    level: str = Field('INFO')
    format: str = Field('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_path: Optional[Path] = Field(None)
    rotation: str = Field('10 MB')

    @field_validator('level')
    def validate_log_level(cls, v: str) -> str:
        v_upper = v.upper()
        if v_upper not in logging._nameToLevel:
            raise ValueError(f"无效的日志级别: {v}")
        return v_upper

class Settings(BaseModel):
    """全局配置主类"""
    model_config = ConfigDict(
        validate_assignment=True,
        frozen=False,
        arbitrary_types_allowed=True
    )
    
    # 单例实例
    _instance: ClassVar[Optional['Settings']] = None

    # 配置字段
    env_state: EnvironmentState = Field(default=EnvironmentState.TESTING)
    fmp: FMPConfig
    telegram: TelegramConfig
    model: ModelConfig
    logging: LoggingConfig

    def validate_production_settings(self) -> None:
        """验证生产环境配置"""
        if self.env_state == EnvironmentState.PRODUCTION:
            token_pattern = re.compile(r'^\d{9}:[\w-]{35}$')
            if not self.telegram.bot_token or not token_pattern.match(self.telegram.bot_token):
                raise ValidationError("生产环境需要提供有效的Telegram机器人令牌")
            
            key_pattern = re.compile(r'^[a-zA-Z0-9]{16,32}$')
            if not key_pattern.match(self.fmp.api_key):
                raise ValidationError("FMP API密钥格式无效")
    
    @classmethod
    def get_instance(cls) -> 'Settings':
        if cls._instance is None:
            raise RuntimeError("Settings尚未初始化，请先调用get_settings()")
        return cls._instance
    
    @classmethod
    def _set_instance(cls, instance: 'Settings') -> None:
        cls._instance = instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """重置单例实例"""
        cls._instance = None

def init_settings(config_file: str = 'conf.yaml') -> tuple[Settings, str]:
    """初始化配置和环境状态"""
    config_path = Path(__file__).parent.parent / config_file
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    try:
        # 首先设置基本的日志配置
        logging.basicConfig(
            level=logging.INFO,  # 默认使用 INFO 级别
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
            
        # 获取环境状态
        env_state = str(config_data.get("env_state", "testing")).lower()
        
        # 根据环境更新日志级别
        if env_state == "development":
            logging.getLogger().setLevel(logging.DEBUG)
            
        # 初始化配置
        settings = Settings.model_validate(config_data)
        settings.validate_production_settings()
        Settings._set_instance(settings)
        
        logging.info("成功加载配置:")
        logging.info(f"环境: {settings.env_state}")
        logging.info(f"模型参数: {settings.model}")
        
        return settings, env_state
        
    except Exception as e:
        logging.error(f"配置初始化失败: {str(e)}")
        raise

def get_settings(force_reload: bool = False) -> Settings:
    """获取配置实例
    
    Args:
        force_reload: 是否强制重新加载配置
        
    Returns:
        Settings: 配置实例
    """
    if not force_reload and Settings._instance is not None:
        return Settings._instance
        
    settings, _ = init_settings()
    return settings

# 程序启动时初始化配置
settings = get_settings()  # 移除对 env_state 的直接引用