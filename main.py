# stock-valuation-tool/main.py
import asyncio
import argparse
from datetime import datetime
import logging
from config.settings import get_settings
from data.fetchers.data_service import DataService
from data.models.monte_carlo import MonteCarloValuator
from notification.telegram_notifier import TelegramNotifier
import sys

logger = logging.getLogger(__name__)

def format_stock_symbol(symbol: str, country: str) -> str:
    """
    格式化股票代码以适应不同市场
    
    Args:
        symbol: 原始股票代码 (例如: AAPL.US 或 5801.JP)
        country: 市场国家代码 (US 或 JP)
    
    Returns:
        str: 格式化后的股票代码
    """
    # 移除可能存在的后缀
    clean_symbol = symbol.split('.')[0]
    
    if country == 'JP':
        return f"TYO:{clean_symbol}"
    elif country == 'US':
        return clean_symbol
    else:
        raise ValueError(f"不支持的市场: {country}")

async def analyze_stock(symbol: str, country: str):
    """股票分析工作流"""
    notifier = TelegramNotifier()
    data_service = DataService()
    
    try:
        # 格式化股票代码
        formatted_symbol = format_stock_symbol(symbol, country)

        # 获取市场数据
        market_data = await data_service.get_market_data(formatted_symbol)
        financials = await data_service.get_financials(formatted_symbol)

        # 执行蒙特卡洛模拟
        valuator = MonteCarloValuator(financials, market_data)
        results = valuator.run_simulation()

        # 格式化并发送结果

                # 解析日期字符串
        date_str = financials.get('date', '')
        try:
            # 截取日期部分，确保只有年月日
            date_part = date_str[:10]
            parsed_date = datetime.strptime(date_part, '%Y-%m-%d').date()
        except ValueError as e:
            logger.error(f"日期解析失败: {date_str}, {str(e)}")
            parsed_date = datetime(1982, 9, 17).date()

        report = {
            'symbol': symbol,
            'date': parsed_date,
            'current_price': market_data['price'],
            'currency': financials['currency'],
            **results['valuation_range'],
            'undervalued_prob': results['probabilities']['undervalued'],
            'overvalued_prob': results['probabilities']['overvalued'],
            'next_quarters': results['next_quarters']
        }
        
        await notifier.send_message(report, 'valuation')
        return report

    except Exception as e:
        error_msg = {
            "module": "主分析流程",
            "error_info": str(e),
            "advice": "检查输入参数或联系系统管理员"
        }
        # 发送错误消息并等待完成
        await notifier.send_message(error_msg, 'error')
        raise
    finally:
        # 确保所有消息发送完成
        await data_service.close()
        await notifier.close()

def main():
    # 首先初始化全局配置
    settings = get_settings()
    
    """命令行入口"""
    parser = argparse.ArgumentParser(description='股票估值分析工具')
    parser.add_argument('-s', '--symbol', required=True, help='股票代码')
    parser.add_argument('-c', '--country', required=True, 
                        choices=['US', 'JP'], help='市场国家')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='显示详细日志')

    args = parser.parse_args()

    # 配置日志
    log_level = logging.DEBUG if args.verbose else settings.logging.level
    logging.basicConfig(
        level=log_level,
        format=settings.logging.format
    )

    try:
        report = asyncio.run(analyze_stock(args.symbol, args.country))
        if args.verbose:
            print("分析结果：")
            print(report)
    except Exception as e:
        logger.exception(f"程序执行失败: {str(e)}")
        exit(1)

if __name__ == "__main__":
    # 开发时加入默认参数，方便调试
    if len(sys.argv) == 1:
         sys.argv.extend(["-s", "AAPL", "-c", "US"])
    main()
