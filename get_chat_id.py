# stock-valuation-tool/get_chat_id.py
import aiohttp
import logging
import asyncio
from typing import Optional
import aiohttp
from config import get_settings
from notification.telegram_notifier import MessageFormatter

settings = get_settings()
logger = logging.getLogger(__name__)

class TelegramGetChatID:

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.session: Optional[aiohttp.ClientSession] = None
        
        self.token = settings.telegram.bot_token
        self.chat_id = settings.telegram.chat_id
        self.timeout = settings.telegram.timeout
        self.parse_mode = settings.telegram.parse_mode

    async def get_chat_id(self, bot_token: str) -> Optional[int]:
        """获取 Telegram Bot 的最新 chat_id"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
                
                async with session.get(url) as response:
                    if response.status != 200:
                        self.logger.error(f"获取更新失败: HTTP {response.status}")
                        return None
                    
                    data = await response.json()
                    if not data.get('ok'):
                        self.logger.error(f"API返回错误: {data}")
                        return None
                    
                    updates = data.get('result', [])
                    if not updates:
                        self.logger.warning("没有找到任何更新，请先与机器人对话")
                        return None
                    
                    latest_update = updates[-1]
                    chat_id = latest_update.get('message', {}).get('chat', {}).get('id')
                    
                    if chat_id:
                        self.logger.info(f"成功获取chat_id: {chat_id}")
                        return chat_id
                    else:
                        self.logger.error("无法从更新中提取chat_id")
                        return None
                    
        except Exception as e:
            self.logger.error(f"获取chat_id时出错: {str(e)}")
            return None

async def main():
    """主函数"""
    settings = get_settings()
    bot_token = settings.telegram.bot_token
    
    # 创建类实例并调用方法
    telegram = TelegramGetChatID()
    chat_id = await telegram.get_chat_id(bot_token)
    
    if chat_id:
        print(f"找到chat_id: {chat_id}")
        print("请将此chat_id添加到配置文件中")
    else:
        print("获取chat_id失败，请确保:")
        print("1. bot_token 正确")
        print("2. 已经和机器人进行过对话")
        print("3. 网络连接正常")


if __name__ == "__main__":
    if settings.env_state == "development":
        logging.getLogger().setLevel(logging.DEBUG)
    asyncio.run(main())