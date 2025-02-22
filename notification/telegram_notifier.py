# notification/telegram_notifier.py
import logging
import asyncio
from typing import Dict, Optional, Any
from aiohttp import ClientSession, ClientError
from config import get_settings
from pydantic import ValidationError
import json
import math

settings = get_settings()
logger = logging.getLogger(__name__)

class NotificationError(Exception):
    """通知系统异常基类"""
    pass

class TelegramNotifier:
    """智能Telegram通知系统（支持Markdown排版和消息队列）"""
    
    def __init__(self):
        self.session: Optional[ClientSession] = None
        self.queue = asyncio.Queue()
        self._message_formatter = MessageFormatter()
        
        # 从配置加载参数
        self.token = settings.telegram.bot_token
        self.chat_id = settings.telegram.chat_id
        self.timeout = settings.telegram.timeout
        self.parse_mode = settings.telegram.parse_mode
        
        # 自动启动后台任务
        self._task = asyncio.create_task(self._process_queue())

    async def _create_session(self):
        """创建可复用的aiohttp会话"""
        if self.session is None or self.session.closed:
            self.session = ClientSession(
                base_url="https://api.telegram.org",
                headers={"Content-Type": "application/json"},
                timeout=self.timeout
            )

    async def send_message(self, content: Dict[str, Any], msg_type: str = 'valuation'):
        """
        发送智能格式化消息
        :param content: 原始数据内容
        :param msg_type: 消息类型（valuation/error/warning）
        """
        formatted = self._message_formatter.format(content, msg_type)
        await self.queue.put(formatted)

    async def _process_queue(self):
        """后台消息队列处理器"""
        while True:
            try:
                message = await self.queue.get()
                await self._safe_send(message)
                await asyncio.sleep(0.1)  # 控制发送频率
            except Exception as e:
                logger.error(f"Queue processing error: {str(e)}")

    async def _safe_send(self, message: str, retries=3):
        """带指数退避的安全发送机制"""
        attempt = 0
        backoff = 2
        
        while attempt < retries:
            try:
                return await self._send_api_request(message)
            except ClientError as e:
                attempt += 1
                wait = backoff ** attempt
                logger.warning(f"Attempt {attempt} failed, retrying in {wait}s: {str(e)}")
                await asyncio.sleep(wait)
            except NotificationError as e:
                logger.error(f"Permanent send failure: {str(e)}")
                break
                
        logger.error(f"Message failed after {retries} attempts: {message[:60]}...")

    async def _send_api_request(self, message: str):
        """执行实际的API请求"""
        await self._create_session()
        
        try:
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
                        raise NotificationError(f"API Error {resp.status}: {error}")
                        
                    response_data = await resp.json()
                    if not response_data.get('ok'):
                        raise NotificationError(f"API Response error: {response_data}")
        except ValidationError as e:
            raise NotificationError(f"Invalid message format: {str(e)}")

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
        """安全关闭资源"""
        if self.session:
            await self.session.close()
        self._task.cancel()

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

    def format(self, data: Dict, msg_type: str) -> str:
        """智能选择模板并格式化"""
        template = self.TEMPLATES.get(msg_type)
        if not template:
            raise ValueError(f"未知的消息类型: {msg_type}")
            
        # 预处理特殊字段
        data.setdefault('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        if msg_type == 'valuation':
            data['forecast_table'] = self._format_forecast(data['next_quarters'])
            data['symbol'] = data.get('symbol', 'UNKNOWN')
            data['market'] = self._detect_market(data.get('currency', 'USD'))
            
        return template.format(**data).strip()

    def _format_forecast(self, forecast: Dict) -> str:
        """生成格式化预测表格"""
        rows = []
        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            low, med, high = forecast[q]
            rows.append(
                f"│ {q.ljust(4)} │ ${low:.2f} │ ${med:.2f} │ ${high:.2f} │"
            )
            
        return (
            "```\n"
            "┌──────┬─────────┬─────────┬─────────┐\n"
            "│ 季度 │ 悲观估值 │ 中性估值 │ 乐观估值 │\n"
            "├──────┼─────────┼─────────┼─────────┤\n" +
            "\n".join(rows) + "\n"
            "└──────┴─────────┴─────────┴─────────┘\n"
            "```"
        )

    def _detect_market(self, currency: str) -> str:
        """根据货币识别市场"""
        return "东京证交所" if currency == 'JPY' else "纽交所/纳斯达克"

# 示例用法
async def demo_send_valuation():
    notifier = TelegramNotifier()
    
    sample_data = {
        "symbol": "AAPL",
        "current_price": 185.25,
        "currency": "USD",
        "Q1": 170.50,
        "median": 192.30,
        "Q4": 210.75,
        "undervalued_prob": 0.35,
        "overvalued_prob": 0.65,
        "next_quarters": {
            "Q1": (175.1, 180.3, 185.5),
            "Q2": (180.5, 188.2, 195.0),
            "Q3": (185.0, 192.3, 200.1),
            "Q4": (190.5, 198.4, 210.2)
        }
    }
    
    await notifier.send_message(sample_data, 'valuation')
    await notifier.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo_send_valuation())
