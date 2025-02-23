# stock-valuation-tool/main.py
import asyncio
import argparse
import logging
from config.settings import get_settings
from data.fetchers.data_service import DataService
from data.models.monte_carlo import MonteCarloValuator
from notification.telegram_notifier import TelegramNotifier
import sys

logger = logging.getLogger(__name__)

async def analyze_stock(symbol: str, country: str):
    """股票分析工作流"""
    notifier = TelegramNotifier()
    data_service = DataService()
    
    try:
        # 获取市场数据（自动故障转移）
        market_data = await data_service.get_market_data(symbol, country)
        financials = await data_service.get_financials(symbol, country)

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
        # 移除对未定义fetcher的引用
        await data_service.close()  # 确保DataService有close方法
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
