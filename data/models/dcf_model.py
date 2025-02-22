# data/models/dcf_model.py
import numpy as np
from typing import Dict
from config import get_settings

settings = get_settings()

class DCFValuation:
    """现金流折现模型（支持多阶段增长）"""
    
    def __init__(self, financials: Dict):
        self.free_cash_flow = float(financials['free_cash_flow'])
        self.growth_rate = None
        self.discount_rate = settings.model.risk_free_rate + 0.05  # 基础折现率
        self.terminal_growth = 0.02
        
    def calculate(self, 
                 growth_rate: float,
                 discount_rate: float,
                 terminal_growth: float) -> float:
        """三阶段DCF计算"""
        # 明确阶段划分
        high_growth_years = settings.model.dcf_growth_years
        transition_years = 3  # 过渡阶段年数
        
        # 高增长阶段
        fcf = self.free_cash_flow
        high_growth = [fcf * (1 + growth_rate)**i 
                      for i in range(1, high_growth_years+1)]
        
        # 过渡阶段
        decline_rate = (growth_rate - terminal_growth) / (transition_years + 1)
        transition_growth = growth_rate
        for _ in range(transition_years):
            transition_growth -= decline_rate
            fcf *= (1 + transition_growth)
            high_growth.append(fcf)
        
        # 永续阶段
        terminal_value = fcf * (1 + terminal_growth) / (discount_rate - terminal_growth)
        
        # 折现计算
        cash_flows = np.array(high_growth + [terminal_value])
        discount_factors = np.array([1 / (1 + discount_rate)**i 
                                   for i in range(1, len(cash_flows)+1)])
        present_value = np.sum(cash_flows * discount_factors)
        
        return present_value
