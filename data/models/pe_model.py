# data/models/pe_model.py
import numpy as np
from scipy import stats
from config import get_settings
from typing import Dict

settings = get_settings()

class PEValuation:
    """动态市盈率估值模型"""
    
    def __init__(self, financials: Dict, industry_pe: float):
        self.net_income = float(financials['net_income'])
        self.industry_pe = industry_pe
        self.historical_pe = None
        
    def calculate(self, pe_ratio: float, earnings_growth: float) -> float:
        """考虑增长调整的PE计算"""
        # 彼得林奇增长调整公式
        adjusted_pe = pe_ratio * (1 + earnings_growth/100)
        # 行业相对溢价调整
        adjusted_pe *= (self.industry_pe / np.mean([self.industry_pe, 15]))
        return self.net_income * adjusted_pe
