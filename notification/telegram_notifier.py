# notification/telegram_notifier.py
import logging
import asyncio
from typing import Dict, Optional, Any, List
from aiohttp import ClientSession, ClientError, ClientTimeout
from config import get_settings
from pydantic import ValidationError
import json
import re

settings = get_settings()

class NotificationError(Exception):
    """通知系统异常基类"""
    pass

class TelegramNotifier:
    """智能Telegram通知系统（支持Markdown排版和消息队列）"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.settings = get_settings()
        
        # 初始化基本属性
        self.token = self.settings.telegram.bot_token
        self.chat_id = self.settings.telegram.chat_id
        self.timeout = ClientTimeout(total=self.settings.telegram.timeout_seconds)
        self.parse_mode = "MarkdownV2"
        
        # 初始化消息队列和会话
        self.queue = asyncio.Queue()
        self.session = None
        self._task = asyncio.create_task(self._process_queue())

        # 初始化消息格式化器
        self._message_formatter = MessageFormatter()
        
    async def _ensure_session(self):
        """确保会话可用"""
        if self.session == None or self.session.closed:
            self.session = ClientSession(timeout=self.timeout)

    async def _validate_config(self) -> bool:
        """验证Telegram配置是否有效"""
        try:
            await self._ensure_session()
            
            # 测试getMe接口验证token
            async with self.session.get(f"/bot{self.token}/getMe") as resp:
                if resp.status != 200:
                    self.logger.error(f"Bot Token无效: HTTP {resp.status}")
                    return False
                    
                bot_info = await resp.json()
                if not bot_info.get('ok'):
                    self.logger.error(f"Bot Token验证失败: {bot_info}")
                    return False
                    
                self.logger.debug(f"Bot验证成功: @{bot_info['result']['username']}")
            
            # 测试chat_id是否有效
            test_msg = "配置测试消息"
            payload = {
                "chat_id": self.chat_id,
                "text": test_msg,
                "disable_notification": True
            }
            
            async with self.session.post(
                f"/bot{self.token}/sendMessage",
                json=payload
            ) as resp:
                if resp.status != 200:
                    self.logger.error(f"Chat ID无效: HTTP {resp.status}")
                    return False
                    
                result = await resp.json()
                if not result.get('ok'):
                    self.logger.error(f"发送测试消息失败: {result}")
                    return False
                    
                self.logger.debug("Chat ID验证成功")
                
            return True
            
        except Exception as e:
            self.logger.error(f"验证配置时出错: {str(e)}")
            return False
            
        finally:
            if self.session:
                await self.session.close()
                
    async def send_message(self, content: Dict[str, Any], msg_type: str) -> None:
        """发送消息并等待发送完成"""
        try:
            formatted = self._message_formatter.format(content, msg_type)
            await self._safe_send(formatted)
            self.logger.info(f"{msg_type}消息发送成功")
            
        except Exception as e:
            error_msg = f"消息发送失败: {str(e)}"
            self.logger.error(error_msg)
            raise NotificationError(error_msg)

    async def _process_queue(self):
        """处理消息队列"""
        try:
            while True:
                # 等待新消息
                message, msg_type = await self.queue.get()
                
                try:
                    await self._safe_send(message)
                    self.logger.info(f"{msg_type}消息发送成功")
                except Exception as e:
                    self.logger.error(f"发送消息失败: {str(e)}")
                finally:
                    # 标记任务完成
                    self.queue.task_done()
                    
        except asyncio.CancelledError:
            self.logger.debug("消息队列处理任务被取消")
        except Exception as e:
            self.logger.error(f"队列处理错误: {str(e)}")

    async def _escape_markdown(self, text: str) -> str:
        """转义 MarkdownV2 特殊字符"""
        special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', 
                        '-', '=', '|', '{', '}', '.', '!']
        for char in special_chars:
            text = text.replace(char, f'\\{char}')
        return text

    async def _safe_send(self, message: str) -> None:
        """安全发送消息"""
        try:
            # 确保会话可用
            await self._ensure_session()
            
            # 转义消息中的特殊字符
            escaped_message = await self._escape_markdown(message)
            
            # 构建请求参数
            params = {
                'chat_id': self.chat_id,
                'text': escaped_message,
                'parse_mode': self.parse_mode
            }
            
            # 构建完整的 URL
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            async with self.session.post(url, json=params) as response:
                if response.status != 200:
                    error_data = await response.json()
                    raise NotificationError(
                        f"发送失败: HTTP {response.status}, {error_data.get('description', '')}"
                    )
                    
                self.logger.info(f"{message.split()[0]}消息发送成功")
                
        except Exception as e:
            self.logger.error(f"发送消息时出错: {str(e)}")
            raise NotificationError(f"发送消息时出错: {str(e)}")

    async def _send_api_request(self, message: str) -> bool:
        """执行实际的API请求"""
        await self._ensure_session()
        
        try:
            success = True
            # 自动分割长消息
            for chunk in self._split_message(message):
                payload = {
                    "chat_id": self.chat_id,
                    "text": chunk,
                    "parse_mode": self.parse_mode,
                    "disable_web_page_preview": True
                }
                
                async with self.session.post(
                    f"/bot{self.token}/sendMessage",
                    data=json.dumps(payload)
                ) as resp:
                    if resp.status != 200:
                        error = await resp.text()
                        raise NotificationError(f"API状态码错误 {resp.status}: {error}")
                        
                    response_data = await resp.json()
                    if not response_data.get('ok'):
                        raise NotificationError(f"API返回错误: {response_data}")
                        
            return success
            
        except ValidationError as e:
            raise NotificationError(f"消息格式无效: {str(e)}")
        except Exception as e:
            raise NotificationError(f"发送消息时发生错误: {str(e)}")

    def _split_message(self, message: str, max_length=4096) -> list:
        """智能分割长消息（保持Markdown结构）"""
        if len(message) <= max_length:
            return [message]
            
        # 在最近的段落分隔符处分割
        split_points = [
            '\n\n## ',  # 二级标题
            '\n\n',     # 段落分隔
            '\n* '      # 列表项
        ]
        
        chunks = []
        while message:
            split_pos = max_length
            for marker in split_points:
                last_pos = message.rfind(marker, 0, max_length)
                if last_pos != -1:
                    split_pos = last_pos
                    break
                    
            chunks.append(message[:split_pos].strip())
            message = message[split_pos:]
            
        return chunks

    async def close(self):
        """关闭通知器"""
        try:
            # 取消队列处理任务
            if hasattr(self, '_task'):
                self._task.cancel()
                await self._task
            
            # 关闭会话
            if self.session and not self.session.closed:
                await self.session.close()
                
        except Exception as e:
            self.logger.error(f"关闭通知器失败: {str(e)}")

class MessageFormatter:
    """多模板消息格式化系统"""
    
    TEMPLATES = {
        'valuation': """
        **{symbol} 估值分析报告** 📈
        🕒 报告时间: {timestamp}
        🌍 市场: {market}
        
        *当前价格*: ${current_price:.2f} ({currency})
        
        **合理估值范围**:
        ```
        ┌─────────────┬───────────┐
        │ 分位点     │ 价格      │
        ├─────────────┼───────────┤
        │ 5% 低估线  │ ${Q1:.2f} │
        │ 中位数     │ ${median:.2f} │
        │ 95% 高估线 │ ${Q4:.2f} │
        └─────────────┴───────────┘
        ```
        
        *市场概率评估*:
        - 📉 低估概率: {undervalued_prob:.1%}
        - 📈 高估概率: {overvalued_prob:.1%}
        
        **未来四季预测**:
        {forecast_table}
        
        _数据来源：IBKR市场数据，蒙特卡洛模拟结果_
        """,
        
        'error': """
        🚨 **系统警报** 🚨
        错误时间: {timestamp}
        模块: {module}
        错误详情:
        ```
        {error_info}
        ```
        建议操作: {advice}
        """,
        
        'warning': """
        ⚠️ **风险预警** ⚠️
        检测时间: {timestamp}
        股票代码: {symbol}
        预警指标:
        {metrics}
        
        建议关注: {advice}
        """
    }

    def _format_next_quarters(self, next_quarters: List[float]) -> str:
        """格式化未来季度预测"""
        try:
            quarters = []
            for i, price in enumerate(next_quarters, 1):
                quarters.append(f"Q{i}: {price:.2f}")
            return "\n".join(quarters) if quarters else "暂无季度预测"
        except Exception as e:
            self.logger.error(f"格式化季度预测失败: {str(e)}")
            return "季度预测格式化错误"

    def format(self, data: Dict[str, Any], msg_type: str) -> str:
        """格式化消息"""
        try:
            if msg_type == 'valuation':
                template = (
                    "*股票估值报告*\n"
                    "代码: `{symbol}`\n"
                    "当前价格: `{currency} {current_price:.2f}`\n\n"
                    "*估值区间*\n"
                    "低估值: `{currency} {low:.2f}`\n"
                    "中位值: `{currency} {medium:.2f}`\n"
                    "高估值: `{currency} {high:.2f}`\n\n"
                    "*概率分析*\n"
                    "低估概率: `{undervalued_prob:.1%}`\n"
                    "高估概率: `{overvalued_prob:.1%}`\n\n"
                    "*未来预测*\n{forecast_table}"
                )
                
                # 格式化季度预测
                data['forecast_table'] = self._format_next_quarters(data['next_quarters'])
                
                return template.format(**data).strip()
            elif msg_type == 'error':
                return (
                    "*错误报告*\n"
                    "模块: `{module}`\n"
                    "错误: `{error_info}`\n"
                    "建议: `{advice}`"
                ).format(**data).strip()
            else:
                raise ValueError(f"不支持的消息类型: {msg_type}")
                
        except Exception as e:
            self.logger.error(f"消息格式化失败: {str(e)}")
            return str(data)  # 作为后备，直接返回原始数据的字符串表示

    def _format_forecast(self, forecast: List[float]) -> str:
        """
        格式化预测数据表格
        
        Args:
            forecast: 未来季度的预测数据列表
            
        Returns:
            str: 格式化后的 Markdown 表格
        """
        try:
            # 表头
            table = "季度 | 预测价格\n"
            table += "---|---\n"
            
            # 添加每个季度的预测值
            for i, price in enumerate(forecast, 1):
                table += f"Q{i} | {price:.2f}\n"
                
            return table
            
        except Exception as e:
            self.logger.error(f"格式化预测数据失败: {str(e)}")
            return "预测数据格式化失败"

    def _detect_market(self, currency: str) -> str:
        """根据货币识别市场"""
        return "东京证交所" if currency == 'JPY' else "纽交所/纳斯达克"

if __name__ == "__main__":
    if settings.env_state == "development":
        logging.getLogger().setLevel(logging.DEBUG)
