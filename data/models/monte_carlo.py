# data/models/monte_carlo.py
import numpy as np
from typing import Tuple, Dict, Any, List
from tqdm import tqdm
from config import get_settings
from .dcf_model import DCFValuation
import logging

settings = get_settings()

class MonteCarloValuator:
    """混合估值蒙特卡洛模拟引擎"""
    
    def __init__(self, financials: Dict[str, Any], market_data: Dict[str, Any]):
        self.logger = logging.getLogger(__name__)
        self.financials = financials
        self.market_data = market_data
        self.settings = settings
        
    def _get_pe(self) -> float:
        """获取股票PE值"""
        default_pe = 15.0

        try:
            pe_ratio = float(self.market_data.get('pe_ratio', 0))
            
            if pe_ratio <= 0 or pe_ratio > 50:  # 添加上限以过滤异常值
                return default_pe
            
            return pe_ratio
                
        except (KeyError, ValueError) as e:
            return default_pe
        
    def run_simulation(self) -> Dict[str, Any]:
        try:
            # 确保价格不为零
            current_price = float(self.market_data.get('price', 0))
            if current_price <= 0:
                raise ValueError("市场价格必须大于0")

            # 运行蒙特卡洛模拟
            simulations = self._run_monte_carlo()
            
            # 计算收益率统计
            mu = np.mean(simulations) / current_price - 1 if current_price > 0 else 0
            sigma = np.std(simulations) / current_price if current_price > 0 else 0
            
            # 计算漂移
            if sigma > 0:
                drift = mu - 0.5 * sigma**2
            else:
                drift = mu
                
            return {
                'valuation_range': {
                    'low': float(np.percentile(simulations, 25)),
                    'medium': float(np.percentile(simulations, 50)),
                    'high': float(np.percentile(simulations, 75))
                },
                'probabilities': self._calculate_probabilities(simulations),
                'next_quarters': self._predict_next_quarters(drift, sigma)
            }
            
        except Exception as e:
            self.logger.error(f"蒙特卡洛模拟失败: {str(e)}")
            raise
        
    def _create_growth_distribution(self) -> Tuple[float, float]:
        """生成营收增长率的正态分布参数"""
        base_growth = 0.05     # 保持基础增长率为5%（行业中性）
        volatility = 0.01      # DCF估值波动，波动率为1%
        return (base_growth, volatility)
        
    def _create_discount_distribution(self) -> Tuple[float, float]:
        """生成折现率的正态分布参数"""
        base_discount = settings.model.risk_free_rate + 0.04  # 风险溢价为4%
        volatility = 0.005     # 降低波动率为0.5%
        return (base_discount, volatility)
        
    def _create_pe_distribution(self) -> Tuple[float, float]:
        """生成PE比率的对数正态分布参数"""
        log_pe = np.log(self._get_pe())
        sigma = 0.15          # 波动率为15%
        return (log_pe, sigma)
        
    def _analyze_results(self, simulations: np.ndarray) -> Dict[str, Any]:
        """分析模拟结果并生成预测"""
        try:
            current_price = float(self.market_data.get('price', 0))
            
            # 限制估值范围在当前价格的0.5-2倍之间
            simulations = np.clip(simulations, current_price * 0.5, current_price * 2.0)
            
            # 计算分位数
            percentiles = np.percentile(simulations, [25, 50, 75])
            
            return {
                'valuation_range': {
                    'low': round(float(percentiles[0]), 2),
                    'medium': round(float(percentiles[1]), 2),
                    'high': round(float(percentiles[2]), 2)
                },
                'probabilities': self._calculate_probabilities(simulations),
                'next_quarters': self._predict_next_quarters(
                    np.mean(simulations) / current_price - 1,  # 漂移率
                    np.std(simulations) / current_price       # 波动率
                )
            }
        except Exception as e:
            self.logger.error(f"分析结果失败: {str(e)}")
            raise
        
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

    def _run_monte_carlo(self) -> np.ndarray:
        """执行蒙特卡洛模拟"""
        try:
            n_sims = self.settings.model.monte_carlo_sims
            simulations = np.zeros(n_sims)
            
            # 获取当前市值作为基准
            current_price = float(self.market_data.get('price', 0))
            if current_price <= 0:
                raise ValueError("无效的当前价格")
                
            # 获取分布参数
            growth_mu, growth_sigma = self._create_growth_distribution()
            discount_mu, discount_sigma = self._create_discount_distribution()
            pe_mu, pe_sigma = self._create_pe_distribution()
            terminal_growth = 0.02
            
            # 使用 tqdm 显示进度条
            for i in tqdm(range(n_sims), desc="运行模拟"):
                # 生成随机参数
                growth = np.random.normal(growth_mu, growth_sigma)
                discount = np.random.normal(discount_mu, discount_sigma)
                pe = np.exp(np.random.normal(pe_mu, pe_sigma))
                
                # DCF 估值 (70% 权重)
                dcf_val = DCFValuation(self.financials).calculate(
                    growth, 
                    discount, 
                    terminal_growth
                )
                
                # PE 估值 (30% 权重)
                eps = float(self.financials.get('eps', 0))
                pe_val = pe * eps
                
                # 组合估值结果，并限制在合理范围内
                combined_val = dcf_val * 0.7 + pe_val * 0.3 

                simulations[i] = np.clip(
                    combined_val,
                    current_price * 0.8,   # 最低80%当前价格
                    current_price * 1.5    # 最高150%当前价格
                )
                
            return simulations
            
        except Exception as e:
            self.logger.error(f"执行蒙特卡洛模拟失败: {str(e)}")
            raise

    def _calculate_probabilities(self, simulations: np.ndarray) -> Dict[str, float]:
        """计算估值概率"""
        try:
            current_price = float(self.market_data.get('price', 0))
            
            # 使用相对价格差计算概率
            price_diff = (simulations - current_price) / current_price
            
            # 设置更严格的阈值
            undervalued_threshold = 0.10  # 低于10%视为低估
            overvalued_threshold = 0.10   # 高于10%视为高估
            
            undervalued = np.mean(price_diff < -undervalued_threshold)
            overvalued = np.mean(price_diff > overvalued_threshold)
            fairly_valued = 1 - undervalued - overvalued
            
            self.logger.debug(f"低估概率: {undervalued:.2%}")
            self.logger.debug(f"公允概率: {fairly_valued:.2%}")
            self.logger.debug(f"高估概率: {overvalued:.2%}")
            
            return {
                'undervalued': round(float(undervalued), 4),
                'overvalued': round(float(overvalued), 4),
                'fair_valued': round(float(fairly_valued), 4)
            }
        except Exception as e:
            self.logger.error(f"计算概率失败: {str(e)}")
            return {'undervalued': 0.0, 'overvalued': 0.0, 'fair_valued': 1.0}

    def _predict_next_quarters(self, drift: float, sigma: float, quarters: int = 4) -> List[float]:
        """预测未来季度股价"""
        try:
            current_price = float(self.market_data.get('price', 0))
            predictions = []
            
            # 限制参数范围
            drift = np.clip(drift, -0.10, 0.10)  # 年化漂移限制从±10%
            sigma = np.clip(sigma, 0.05, 0.20)   # 波动率限制从5%-20%
            
            price = current_price
            for _ in range(quarters):
                # 生成随机波动
                z = np.random.normal()
                # 计算季度收益率
                quarterly_return = (drift/4) + (sigma/2) * z
                # 更新价格
                price = price * (1 + quarterly_return)
                # 限制单季度价格变动范围
                price = np.clip(price, current_price * 0.8, current_price * 1.2)
                predictions.append(round(float(price), 2))
            
            self.logger.debug(f"预测未来{quarters}个季度价格: {predictions}")
            return predictions
            
        except Exception as e:
            self.logger.error(f"季度预测失败: {str(e)}")
            return [round(current_price, 2)] * quarters
