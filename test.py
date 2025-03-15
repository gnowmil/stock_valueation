import logging
import asyncio
from data.fetchers import AKShareFetcher

# 配置日誌
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    fetcher = AKShareFetcher()
    
    try:
        # 檢查數據源可用性
        if not await fetcher.is_available():
            print("數據源不可用")
            return
            
        # 測試不同市場的數據獲取
        symbols = [
            ("600519.SH", "貴州茅台"),
            ("00700.HK", "騰訊控股"),
            ("AAPL", "蘋果公司"),
            ("7203.T", "豐田汽車")
        ]
        
        for symbol, name in symbols:
            print(f"\n正在獲取 {name}({symbol}) 的數據...")
            try:
                data = await fetcher.fetch_market_data(symbol)
                print(f"{name} 的數據: {data}")
            except Exception as e:
                print(f"獲取 {name} 數據時出錯: {str(e)}")
    except Exception as e:
        print(f"主程序發生錯誤: {e}")

if __name__ == "__main__":
    asyncio.run(main())