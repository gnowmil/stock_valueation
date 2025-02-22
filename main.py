# stock-valuation-tool/main.py
import asyncio
import argparse
import logging
from config.settings import get_settings
from data.fetchers.ibkr_fetcher import IBKRFetcher
from data.models.monte_carlo import MonteCarloValuator
from notification.telegram_notifier import TelegramNotifier
import sys

logger = logging.getLogger(__name__)

async def analyze_stock(symbol: str, country: str):
    """股票分析工作流"""
    fetcher = IBKRFetcher()
    notifier = TelegramNotifier()
    
    try:
        # 连接IBKR
        if not await fetcher.connect():
            raise ConnectionError("无法连接IBKR Gateway")

        # 获取数据
        market_data = await fetcher.fetch_market_data(symbol, country)
        financials = await fetcher.fetch_financials(symbol, country)
        
        if not all([market_data['price'], financials['net_income']]):
            raise ValueError("关键数据缺失")

        # 准备估值参数
        financials.update({
            'shares_outstanding': 1e9  # 应从SEC/EDINET获取实际值
        })

        # 执行蒙特卡洛模拟
        valuator = MonteCarloValuator(financials, market_data)
        results = valuator.run_simulation()

        # 格式化并发送结果
        report = {
            'symbol': symbol,
            'current_price': market_data['price'],
            'currency': market_data['currency'],
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
        await notifier.send_message(error_msg, 'error')
        raise
    finally:
        await fetcher.safe_shutdown()
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
        logger.critical(f"程序执行失败: {str(e)}")
        exit(1)

if __name__ == "__main__":
    # 开发时加入默认参数，方便调试
    if len(sys.argv) == 1:
         sys.argv.extend(["-s", "AAPL", "-c", "US"])
    main()
