# data/models/monte_carlo.py
import numpy as np
import pandas as pd
from typing import Tuple, Dict
from tqdm import tqdm
from config import get_settings
from .dcf_model import DCFValuation
from .pe_model import PEValuation

settings = get_settings()

class MonteCarloValuator:
    """混合估值蒙特卡洛模拟引擎"""
    
    def __init__(self, financials: Dict, market_data: Dict):
        self.financials = financials
        self.market_data = market_data
        self.industry_pe = self._get_industry_pe()
        
    def _get_industry_pe(self) -> float:
        # 此处可接入行业数据API
        return 18.0  # 示例值
        
    def run_simulation(self, n_sims: int = None) -> Dict:
        """执行混合模型模拟"""
        n_sims = n_sims or settings.model.monte_carlo_sims
        results = {'dcf': [], 'pe': []}
        
        # 定义参数分布
        growth_dist = self._create_growth_distribution()
        discount_dist = self._create_discount_distribution()
        pe_dist = self._create_pe_distribution()
        
        for _ in tqdm(range(n_sims), desc="Running Simulations"):
            # DCF参数抽样
            growth = np.random.normal(*growth_dist)
            discount = np.random.normal(*discount_dist)
            terminal_growth = np.random.uniform(0.01, 0.03)
            
            # PE参数抽样
            pe = np.random.lognormal(*pe_dist)
            earnings_growth = np.random.normal(growth*0.8, 0.02)  # 利润增速与收入增速相关
            
            # 计算估值
            dcf_val = DCFValuation(self.financials).calculate(growth, discount, terminal_growth)
            pe_val = PEValuation(self.financials, self.industry_pe).calculate(pe, earnings_growth)
            
            # 转换为股价
            dcf_price = dcf_val / self.market_data['shares_outstanding']
            pe_price = pe_val / self.market_data['shares_outstanding']
            
            results['dcf'].append(dcf_price)
            results['pe'].append(pe_price)
            
        return self._analyze_results(results)
        
    def _create_growth_distribution(self) -> Tuple[float, float]:
        """生成营收增长率的正态分布参数"""
        base_growth = 0.05
        volatility = 0.02
        return (base_growth, volatility)
        
    def _create_discount_distribution(self) -> Tuple[float, float]:
        """生成折现率的正态分布参数"""
        base_discount = settings.model.risk_free_rate + 0.05
        volatility = 0.01
        return (base_discount, volatility)
        
    def _create_pe_distribution(self) -> Tuple[float, float]:
        """生成PE比率的对数正态分布参数"""
        log_pe = np.log(self.industry_pe)
        sigma = 0.2
        return (log_pe, sigma)
        
    def _analyze_results(self, results: Dict) -> Dict:
        """分析模拟结果并生成预测"""
        combined = np.array(results['dcf']) * 0.6 + np.array(results['pe']) * 0.4
        
        # 计算分位数
        percentiles = np.percentile(combined, [5, 25, 50, 75, 95])
        
        return {
            'current_price': self.market_data['price'],
            'valuation_range': {
                'Q1': percentiles[0],
                'Q2': percentiles[1],
                'median': percentiles[2],
                'Q3': percentiles[3],
                'Q4': percentiles[4]
            },
            'probabilities': {
                'undervalued': np.mean(combined < self.market_data['price']),
                'overvalued': np.mean(combined > self.market_data['price'])
            },
            'next_quarters': self._forecast_quarters(combined)
        }
        
    def _forecast_quarters(self, simulations: np.ndarray) -> Dict:
        """预测未来四季度价格路径"""
        # 使用几何布朗运动模拟
        mu = np.mean(simulations) / self.market_data['price'] - 1
        sigma = np.std(simulations) / self.market_data['price']
        
        paths = []
        for _ in range(1000):
            prices = [self.market_data['price']]
            for _ in range(4):
                drift = mu - 0.5 * sigma**2
                shock = sigma * np.random.normal()
                prices.append(prices[-1] * np.exp(drift + shock))
            paths.append(prices[1:])  # 排除当前价格
            
        return {
            'Q1': np.percentile([p[0] for p in paths], [25, 50, 75]),
            'Q2': np.percentile([p[1] for p in paths], [25, 50, 75]),
            'Q3': np.percentile([p[2] for p in paths], [25, 50, 75]),
            'Q4': np.percentile([p[3] for p in paths], [25, 50, 75])
        }
