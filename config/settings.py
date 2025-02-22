import os
import logging
import yaml
from pathlib import Path
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator, ValidationError

class EnvironmentState(str, Enum):
    """运行环境状态枚举"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"

class IBKRConfig(BaseModel):
    """IBKR网关配置项"""
    host: str = Field(..., description="IBKR网关主机地址")
    port: int = Field(4001, description="IBKR网关端口")
    client_id: int = Field(1, description="客户端ID")
    timeout: int = Field(30, description="连接超时时间")
    read_only: bool = Field(True, description="是否只读模式")
    account: Optional[str] = Field(None, description="IBKR账户号")
    
    @field_validator('read_only')
    def validate_readonly(cls, v: Any) -> bool:
        if isinstance(v, bool):
            return v
        return str(v).lower() in {'true', '1', 'yes'}
    
    @field_validator('port')
    def validate_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError("端口号必须在1-65535范围内")
        return v

class TelegramConfig(BaseModel):
    """Telegram通知配置"""
    bot_token: str = Field(..., description="机器人API令牌")
    chat_id: int = Field(..., description="聊天ID")
    timeout: int = Field(10, description="请求超时时间")
    parse_mode: str = Field('MarkdownV2', description="消息解析模式")

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
    _instance = None
    # 添加环境状态字段
    env_state: EnvironmentState = Field(
        default=EnvironmentState.DEVELOPMENT,
        description="运行环境状态"
    )
    ibkr: IBKRConfig
    telegram: TelegramConfig
    model: ModelConfig
    logging: LoggingConfig

    @classmethod
    def get_instance(cls) -> 'Settings':
        """获取Settings单例实例"""
        if cls._instance is None:
            raise RuntimeError("Settings尚未初始化，请先调用get_settings()")
        return cls._instance
    
    @classmethod
    def _set_instance(cls, instance: 'Settings') -> None:
        """设置Settings单例实例"""
        cls._instance = instance

def get_settings(config_file: str = 'conf.yaml') -> Settings:
    """动态获取配置实例"""
    if Settings._instance is not None:
        return Settings._instance

    config_path = Path(__file__).parent.parent / config_file
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
            
        settings = Settings.model_validate(config_data)
        settings.validate_production_settings()
        Settings._set_instance(settings)
        return settings
    except yaml.YAMLError as e:
        logging.error(f"YAML解析错误: {e}")
        raise
    except ValidationError as e:
        logging.error(f"配置验证失败: {e}")
        raise

# 配置日志
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')

# 初始化配置
try:
    settings = get_settings()
    logging.info(f"成功加载配置")
    logging.info(f"环境: {settings.env_state}")
    logging.info(f"IBKR主机: {settings.ibkr.host}")
    logging.info(f"模型参数: {settings.model}")
except Exception as e:
    logging.error(f"配置初始化失败: {str(e)}")
    raise