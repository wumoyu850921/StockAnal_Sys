# -*- coding: utf-8 -*-
"""
智能分析系统（股票） - 股票市场数据分析系统
修改：熊猫大侠
版本：v2.1.0
许可证：MIT License
"""
# stock_analyzer.py
import time
import traceback
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import requests
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
import logging
import math
import json
import threading
import pymysql
import akshare as ak

# Thread-local storage
thread_local = threading.local()


class StockAnalyzer:
    """
    股票分析器 - 原有API保持不变，内部实现增强
    """

    def __init__(self, initial_cash=1000000):
        # 设置日志
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # 加载环境变量
        load_dotenv()

        # MySQL数据库配置
        self.mysql_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'daiqiang',
            'database': 'stock',
            'charset': 'utf8mb4'
        }

        # 设置 OpenAI API (原来是Gemini API)
        self.openai_api_key = os.getenv('OPENAI_API_KEY', os.getenv('OPENAI_API_KEY'))
        self.openai_api_url = os.getenv('OPENAI_API_URL', 'https://api.openai.com/v1')
        self.openai_model = os.getenv('OPENAI_API_MODEL', 'gemini-2.0-pro-exp-02-05')
        self.news_model = os.getenv('NEWS_MODEL')

        # 配置参数
        self.params = {
            'ma_periods': {'short': 5, 'medium': 20, 'long': 60},
            'rsi_period': 14,
            'bollinger_period': 20,
            'bollinger_std': 2,
            'volume_ma_period': 20,
            'atr_period': 14
        }

        # 添加缓存初始化
        self.data_cache = {}
        
        # CAN SLIM评分缓存
        self.can_slim_cache = {}
        self.can_slim_cache_duration = 3600  # 1小时缓存时间（秒）
        self.can_slim_cache_file = 'can_slim_cache.json'
        
        # 市场扫描缓存
        self.market_scan_cache = {}
        self.market_scan_cache_duration = 3600  # 1小时缓存时间（秒）
        self.market_scan_cache_file = 'market_scan_cache.json'

    def get_mysql_connection(self):
        """获取MySQL数据库连接"""
        try:
            connection = pymysql.connect(**self.mysql_config)
            return connection
        except Exception as e:
            self.logger.error(f"MySQL连接失败: {e}")
            return None

    def get_stock_data_from_db(self, stock_code, start_date=None, end_date=None):
        """从MySQL数据库获取A股历史数据"""
        connection = self.get_mysql_connection()
        if not connection:
            return None
        
        try:
            # 构建SQL查询，返回与AKShare相同的中文列名格式
            sql = """
            SELECT stock_code, trade_date as 日期, open_price as 开盘, high_price as 最高, 
                   low_price as 最低, close_price as 收盘, volume as 成交量, turnover as 成交额,
                   amplitude as 振幅, change_pct as 涨跌幅, change_amount as 涨跌额, 
                   turnover_rate as 换手率
            FROM stock_history_data 
            WHERE stock_code = %s
            """
            
            params = [stock_code]
            
            # 添加日期条件
            if start_date:
                sql += " AND trade_date >= %s"
                # 将YYYYMMDD格式转换为YYYY-MM-DD
                if len(start_date) == 8:
                    formatted_start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
                else:
                    formatted_start = start_date
                params.append(formatted_start)
                
            if end_date:
                sql += " AND trade_date <= %s"
                # 将YYYYMMDD格式转换为YYYY-MM-DD
                if len(end_date) == 8:
                    formatted_end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
                else:
                    formatted_end = end_date
                params.append(formatted_end)
            
            sql += " ORDER BY trade_date ASC"
            
            # 执行查询
            df = pd.read_sql(sql, connection, params=params)
            
            if df.empty:
                self.logger.warning(f"数据库中未找到股票 {stock_code} 的历史数据")
                return None
            
            self.logger.info(f"从数据库获取股票 {stock_code} 数据: {len(df)} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"从数据库获取股票数据失败: {e}")
            return None
        finally:
            connection.close()

    def get_stock_data(self, stock_code, market_type='A', start_date=None, end_date=None):
        """获取股票数据"""

        self.logger.info(f"开始获取股票 {stock_code} 数据，市场类型: {market_type}")

        cache_key = f"{stock_code}_{market_type}_{start_date}_{end_date}_price"
        if cache_key in self.data_cache:
            cached_df = self.data_cache[cache_key]
            # Create a copy to avoid modifying the cached data
            # and ensure date is datetime type for the copy
            result = cached_df.copy()
            # If 'date' column exists but is not datetime, convert it
            if 'date' in result.columns and not pd.api.types.is_datetime64_any_dtype(result['date']):
                try:
                    result['date'] = pd.to_datetime(result['date'])
                except Exception as e:
                    self.logger.warning(f"无法将日期列转换为datetime格式: {str(e)}")
            return result

        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logging.info(f"获取股票 {stock_code} 的历史数据，市场: {market_type}, 起始日期: {start_date}, 结束日期: {end_date}")

        try:
            # 根据市场类型获取数据
            if market_type == 'A':
                logging.info(f"直接从AKShare获取股票 {stock_code} 的历史数据")
                df = ak.stock_zh_a_hist(
                    symbol=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )
            elif market_type == 'HK':
                df = ak.stock_hk_daily(
                    symbol=stock_code,
                    adjust="qfq"
                )
            elif market_type == 'US':
                df = ak.stock_us_hist(
                    symbol=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )
            else:
                raise ValueError(f"不支持的市场类型: {market_type}")

            logging.info(f"股票 {stock_code} 原始数据: shape={df.shape}, columns={list(df.columns)}, 最新5条记录:\n{df.head()}")

            # 重命名列名以匹配分析需求
            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount"
            })

            # 确保日期格式正确
            df['date'] = pd.to_datetime(df['date'])

            # 数据类型转换
            numeric_columns = ['open', 'close', 'high', 'low', 'volume']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # 删除空值
            df = df.dropna()

            result = df.sort_values('date')

            # 缓存原始数据（包含datetime类型）
            self.data_cache[cache_key] = result.copy()

            return result

        except Exception as e:
            self.logger.error(f"获取股票数据失败: {e}")
            raise Exception(f"获取股票数据失败: {e}")

    def get_north_flow_history(self, stock_code, start_date=None, end_date=None):
        """获取单个股票的北向资金历史持股数据"""
        try:
            import akshare as ak

            # 获取历史持股数据
            if start_date is None and end_date is None:
                # 默认获取近90天数据
                north_hist_data = ak.stock_hsgt_hist_em(symbol=stock_code)
            else:
                north_hist_data = ak.stock_hsgt_hist_em(symbol=stock_code, start_date=start_date, end_date=end_date)

            if north_hist_data.empty:
                return {"history": []}

            # 转换为列表格式返回
            history = []
            for _, row in north_hist_data.iterrows():
                history.append({
                    "date": row.get('日期', ''),
                    "holding": float(row.get('持股数', 0)) if '持股数' in row else 0,
                    "ratio": float(row.get('持股比例', 0)) if '持股比例' in row else 0,
                    "change": float(row.get('持股变动', 0)) if '持股变动' in row else 0,
                    "market_value": float(row.get('持股市值', 0)) if '持股市值' in row else 0
                })

            return {"history": history}
        except Exception as e:
            self.logger.error(f"获取北向资金历史数据出错: {str(e)}")
            return {"history": []}

    def calculate_ema(self, series, period):
        """计算指数移动平均线"""
        return series.ewm(span=period, adjust=False).mean()

    def calculate_rsi(self, series, period):
        """计算RSI指标"""
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def calculate_macd(self, series):
        """计算MACD指标"""
        exp1 = series.ewm(span=12, adjust=False).mean()
        exp2 = series.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        hist = macd - signal
        return macd, signal, hist

    def calculate_bollinger_bands(self, series, period, std_dev):
        """计算布林带"""
        middle = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        upper = middle + (std * std_dev)
        lower = middle - (std * std_dev)
        return upper, middle, lower

    def calculate_atr(self, df, period):
        """计算ATR指标"""
        high = df['high']
        low = df['low']
        close = df['close'].shift(1)

        tr1 = high - low
        tr2 = abs(high - close)
        tr3 = abs(low - close)

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()

    def format_indicator_data(self, df):
        """格式化指标数据，控制小数位数"""

        # 格式化价格数据 (2位小数)
        price_columns = ['open', 'close', 'high', 'low', 'MA5', 'MA20', 'MA60', 'BB_upper', 'BB_middle', 'BB_lower']
        for col in price_columns:
            if col in df.columns:
                df[col] = df[col].round(2)

        # 格式化MACD相关指标 (3位小数)
        macd_columns = ['MACD', 'Signal', 'MACD_hist']
        for col in macd_columns:
            if col in df.columns:
                df[col] = df[col].round(3)

        # 格式化其他技术指标 (2位小数)
        other_columns = ['RSI', 'Volatility', 'ROC', 'Volume_Ratio']
        for col in other_columns:
            if col in df.columns:
                df[col] = df[col].round(2)

        return df

    def calculate_indicators(self, df):
        """计算技术指标"""

        try:
            # 计算移动平均线
            df['MA5'] = self.calculate_ema(df['close'], self.params['ma_periods']['short'])
            df['MA20'] = self.calculate_ema(df['close'], self.params['ma_periods']['medium'])
            df['MA60'] = self.calculate_ema(df['close'], self.params['ma_periods']['long'])

            # 计算RSI
            df['RSI'] = self.calculate_rsi(df['close'], self.params['rsi_period'])

            # 计算MACD
            df['MACD'], df['Signal'], df['MACD_hist'] = self.calculate_macd(df['close'])

            # 计算布林带
            df['BB_upper'], df['BB_middle'], df['BB_lower'] = self.calculate_bollinger_bands(
                df['close'],
                self.params['bollinger_period'],
                self.params['bollinger_std']
            )

            # 成交量分析
            df['Volume_MA'] = df['volume'].rolling(window=self.params['volume_ma_period']).mean()
            df['Volume_Ratio'] = df['volume'] / df['Volume_MA']

            # 计算ATR和波动率
            df['ATR'] = self.calculate_atr(df, self.params['atr_period'])
            df['Volatility'] = df['ATR'] / df['close'] * 100

            # 动量指标
            df['ROC'] = df['close'].pct_change(periods=10) * 100

            # 格式化数据
            df = self.format_indicator_data(df)

            return df

        except Exception as e:
            self.logger.error(f"计算技术指标时出错: {str(e)}")
            raise

    def calculate_score(self, df, market_type='A'):
        """
        Calculate stock score - Enhanced with Multi-timeframe Risk Weight Adjustment
        新增短期风险预警系统，提高评分系统的实时性和准确性
        """
        try:
            score = 0
            latest = df.iloc[-1]
            prev_days = min(30, len(df) - 1)

            # 方案一：多时间框架风险权重调整
            # 优化后的权重配置 - 降低历史趋势依赖，增加风险预警
            weights = {
                'trend': 0.25,      # 降低历史趋势权重 (从30%降至25%)
                'volatility': 0.15, # 波动性权重保持
                'technical': 0.25,  # 技术指标权重保持
                'volume': 0.20,     # 成交量权重保持
                'momentum': 0.10,   # 动量权重保持
                'risk_warning': 0.05  # 新增风险预警维度
            }

            # 短期风险信号检测函数
            def detect_short_term_risks(df, latest_data):
                """
                检测短期风险信号，提高评分系统的实时预警能力
                """
                risk_penalty = 0
                
                try:
                    # 获取前几日数据用于对比分析
                    if len(df) >= 5:
                        prev_5_data = df.tail(6).iloc[:-1]  # 过去5日数据
                        prev_day = df.iloc[-2] if len(df) >= 2 else latest_data
                        
                        # 1. 放量滞涨检测 (高风险信号)
                        if len(prev_5_data) > 0:
                            avg_volume = prev_5_data['volume'].mean()
                            volume_ratio = latest_data['volume'] / avg_volume if avg_volume > 0 else 1
                            price_change = (latest_data['close'] - prev_day['close']) / prev_day['close']
                            
                            # 成交量放大50%以上，但涨幅小于1%，警示抛压
                            if volume_ratio > 1.5 and price_change < 0.01:
                                risk_penalty -= 15
                                self.logger.info(f"检测到放量滞涨风险: 成交量比例{volume_ratio:.2f}, 涨幅{price_change:.3f}")
                        
                        # 2. 上影线检测 (卖压信号)
                        upper_shadow_ratio = (latest_data['high'] - max(latest_data['open'], latest_data['close'])) / latest_data['close']
                        if upper_shadow_ratio > 0.025:  # 上影线超过2.5%
                            risk_penalty -= min(10, upper_shadow_ratio * 400)  # 最大扣10分
                            self.logger.info(f"检测到上影线风险: 上影线比例{upper_shadow_ratio:.3f}")
                        
                        # 3. 连续下跌检测 (趋势恶化)
                        if len(df) >= 3:
                            recent_changes = []
                            for i in range(len(df)-3, len(df)):
                                if i > 0:
                                    change = (df.iloc[i]['close'] - df.iloc[i-1]['close']) / df.iloc[i-1]['close']
                                    recent_changes.append(change)
                            
                            # 连续两日跌幅超过1%，扣分加重
                            if len(recent_changes) >= 2 and all(c < -0.01 for c in recent_changes[-2:]):
                                risk_penalty -= 20
                                self.logger.info(f"检测到连续下跌风险: 最近涨跌幅{recent_changes}")
                        
                        # 4. 量价背离检测 (动能衰竭)
                        if len(df) >= 5:
                            # 计算过去5日价格和成交量趋势
                            price_trend = (df.tail(5)['close'].iloc[-1] - df.tail(5)['close'].iloc[0]) / df.tail(5)['close'].iloc[0]
                            volume_trend = (df.tail(5)['volume'].iloc[-1] - df.tail(5)['volume'].iloc[0]) / df.tail(5)['volume'].iloc[0]
                            
                            # 价格上涨但成交量明显萎缩 = 上涨乏力
                            if price_trend > 0.02 and volume_trend < -0.3:
                                risk_penalty -= 12
                                self.logger.info(f"检测到量价背离风险: 价格趋势{price_trend:.3f}, 成交量趋势{volume_trend:.3f}")
                
                except Exception as e:
                    self.logger.warning(f"风险信号检测出错: {str(e)}")
                
                return max(-50, risk_penalty)  # 风险扣分最大50分
            
            # 执行短期风险检测
            risk_penalty = detect_short_term_risks(df, latest)

            # 根据市场类型调整权重
            if market_type == 'US':
                # US stocks prioritize long-term trends but reduce risk weight
                weights['trend'] = 0.30
                weights['volatility'] = 0.10
                weights['momentum'] = 0.12
                weights['risk_warning'] = 0.03
            elif market_type == 'HK':
                # HK stocks adjust for volatility and volume, increase risk awareness
                weights['volatility'] = 0.20
                weights['volume'] = 0.22
                weights['risk_warning'] = 0.08

            # 1. 优化后的趋势评分 (25分满分，降低历史依赖)
            trend_score = 0

            # 均线排列评估 - 降低权重，更关注短期变化
            if latest['MA5'] > latest['MA20'] and latest['MA20'] > latest['MA60']:
                # 多头排列 (12分，从15分调整)
                trend_score += 12
            elif latest['MA5'] > latest['MA20']:
                # 短期上升趋势 (8分，从10分调整)
                trend_score += 8
            elif latest['MA20'] > latest['MA60']:
                # 中期上升趋势 (4分，从5分调整)
                trend_score += 4

            # 价格位置评估 - 更注重当前表现
            if latest['close'] > latest['MA5']:
                trend_score += 4  # 从5分调整为4分
            if latest['close'] > latest['MA20']:
                trend_score += 4  # 从5分调整为4分
            if latest['close'] > latest['MA60']:
                trend_score += 3  # 从5分调整为3分
                
            # 新增：短期动能评估
            if len(df) >= 3:
                recent_3_change = (latest['close'] - df.iloc[-4]['close']) / df.iloc[-4]['close'] if len(df) >= 4 else 0
                if recent_3_change > 0.02:  # 近3日涨幅超过2%
                    trend_score += 2
                elif recent_3_change < -0.02:  # 近3日跌幅超过2%
                    trend_score -= 2

            # 确保最大评分限制 (25分满分)
            trend_score = max(0, min(25, trend_score))

            # 2. Volatility Score (15 points max) - Dimension 2: Filtering
            volatility_score = 0

            # Moderate volatility is optimal
            volatility = latest['Volatility']
            if 1.0 <= volatility <= 2.5:
                # Optimal volatility, best case
                volatility_score += 15
            elif 2.5 < volatility <= 4.0:
                # Higher volatility, second best
                volatility_score += 10
            elif volatility < 1.0:
                # Too low volatility, lacks energy
                volatility_score += 5
            else:
                # Too high volatility, high risk
                volatility_score += 0

            # 3. Technical Indicator Score (25 points max) - "Peak Detection System"
            technical_score = 0

            # RSI indicator evaluation (10 points)
            rsi = latest['RSI']
            if 40 <= rsi <= 60:
                # Neutral zone, stable trend
                technical_score += 7
            elif 30 <= rsi < 40 or 60 < rsi <= 70:
                # Threshold zone, potential reversal signals
                technical_score += 10
            elif rsi < 30:
                # Oversold zone, potential buying opportunity
                technical_score += 8
            elif rsi > 70:
                # Overbought zone, potential selling risk
                technical_score += 2

            # MACD indicator evaluation (10 points) - "Peak Warning Signal"
            if latest['MACD'] > latest['Signal'] and latest['MACD_hist'] > 0:
                # MACD golden cross and positive histogram
                technical_score += 10
            elif latest['MACD'] > latest['Signal']:
                # MACD golden cross
                technical_score += 8
            elif latest['MACD'] < latest['Signal'] and latest['MACD_hist'] < 0:
                # MACD death cross and negative histogram
                technical_score += 0
            elif latest['MACD_hist'] > df.iloc[-2]['MACD_hist']:
                # MACD histogram increasing, potential reversal signal
                technical_score += 5

            # Bollinger Band position evaluation (5 points)
            bb_position = (latest['close'] - latest['BB_lower']) / (latest['BB_upper'] - latest['BB_lower'])
            if 0.3 <= bb_position <= 0.7:
                # Price in middle zone of Bollinger Bands, stable trend
                technical_score += 3
            elif bb_position < 0.2:
                # Price near lower band, potential oversold
                technical_score += 5
            elif bb_position > 0.8:
                # Price near upper band, potential overbought
                technical_score += 1

            # Ensure maximum score limit
            technical_score = min(25, technical_score)

            # 4. Volume Score (20 points max) - "Energy Conservation Dimension"
            volume_score = 0

            # Volume trend analysis
            recent_vol_ratio = [df.iloc[-i]['Volume_Ratio'] for i in range(1, min(6, len(df)))]
            avg_vol_ratio = sum(recent_vol_ratio) / len(recent_vol_ratio)

            if avg_vol_ratio > 1.5 and latest['close'] > df.iloc[-2]['close']:
                # Volume surge with price increase - "volume energy threshold breakthrough"
                volume_score += 20
            elif avg_vol_ratio > 1.2 and latest['close'] > df.iloc[-2]['close']:
                # Volume and price rising together
                volume_score += 15
            elif avg_vol_ratio < 0.8 and latest['close'] < df.iloc[-2]['close']:
                # Decreasing volume with price decrease, potentially healthy correction
                volume_score += 10
            elif avg_vol_ratio > 1.2 and latest['close'] < df.iloc[-2]['close']:
                # Volume increasing with price decrease, potentially heavy selling pressure
                volume_score += 0
            else:
                # Other situations
                volume_score += 8

            # 5. Momentum Score (10 points max) - Dimension 1: Weekly timeframe
            momentum_score = 0

            # ROC momentum indicator
            roc = latest['ROC']
            if roc > 5:
                # Strong upward momentum
                momentum_score += 10
            elif 2 <= roc <= 5:
                # Moderate upward momentum
                momentum_score += 8
            elif 0 <= roc < 2:
                # Weak upward momentum
                momentum_score += 5
            elif -2 <= roc < 0:
                # Weak downward momentum
                momentum_score += 3
            else:
                # Strong downward momentum
                momentum_score += 0

            # 优化后的综合评分计算 - 包含风险预警系统
            # 基础评分计算 (基于新的权重配置)
            base_score = (
                    trend_score * weights['trend'] / 0.25 +
                    volatility_score * weights['volatility'] / 0.15 +
                    technical_score * weights['technical'] / 0.25 +
                    volume_score * weights['volume'] / 0.20 +
                    momentum_score * weights['momentum'] / 0.10
            )
            
            # 应用风险预警扣分 (方案一的核心改进)
            risk_adjusted_score = base_score + (risk_penalty * weights['risk_warning'] / 0.05)
            
            # 记录详细评分信息用于调试
            self.logger.info(f"评分详情 - 趋势:{trend_score}, 波动:{volatility_score}, 技术:{technical_score}, 成交量:{volume_score}, 动量:{momentum_score}, 风险扣分:{risk_penalty}")
            
            final_score = risk_adjusted_score

            # Special market adjustments - "Market Adaptation Mechanism"
            if market_type == 'US':
                # US market additional adjustment factors
                # Check if it's earnings season
                is_earnings_season = self._is_earnings_season()
                if is_earnings_season:
                    # Earnings season has higher volatility, adjust score certainty
                    final_score = 0.9 * final_score + 5  # Slight regression to the mean

            elif market_type == 'HK':
                # HK stocks special adjustment
                # Check for A-share linkage effect
                a_share_linkage = self._check_a_share_linkage(df)
                if a_share_linkage > 0.7:  # High linkage
                    # Adjust based on mainland market sentiment
                    mainland_sentiment = self._get_mainland_market_sentiment()
                    if mainland_sentiment > 0:
                        final_score += 5
                    else:
                        final_score -= 5

            # Ensure score remains within 0-100 range
            final_score = max(0, min(100, round(final_score)))

            # Store sub-scores for display
            self.score_details = {
                'trend': trend_score,
                'volatility': volatility_score,
                'technical': technical_score,
                'volume': volume_score,
                'momentum': momentum_score,
                'total': final_score
            }

            return final_score

        except Exception as e:
            self.logger.error(f"Error calculating score: {str(e)}")
            # Return neutral score on error
            return 50

    def calculate_position_size(self, stock_code, risk_percent=2.0, stop_loss_percent=5.0):
        """
        Calculate optimal position size based on risk management principles
        Implements the "Position Sizing Formula" from Time-Space Resonance System

        Parameters:
            stock_code: Stock code to analyze
            risk_percent: Percentage of total capital to risk on this trade (default 2%)
            stop_loss_percent: Stop loss percentage from entry point (default 5%)

        Returns:
            Position size as percentage of total capital
        """
        try:
            # Get stock data
            df = self.get_stock_data(stock_code)
            df = self.calculate_indicators(df)

            # Get volatility factor (from dimension 3: Energy Conservation)
            latest = df.iloc[-1]
            volatility = latest['Volatility']

            # Calculate volatility adjustment factor (higher volatility = smaller position)
            volatility_factor = 1.0
            if volatility > 4.0:
                volatility_factor = 0.6  # Reduce position for high volatility stocks
            elif volatility > 2.5:
                volatility_factor = 0.8  # Slightly reduce position
            elif volatility < 1.0:
                volatility_factor = 1.2  # Can increase position for low volatility stocks

            # Calculate position size using risk formula
            # Formula: position_size = (risk_amount) / (stop_loss * volatility_factor)
            position_size = (risk_percent) / (stop_loss_percent * volatility_factor)

            # Limit maximum position to 25% for diversification
            position_size = min(position_size, 25.0)

            return position_size

        except Exception as e:
            self.logger.error(f"Error calculating position size: {str(e)}")
            # Return conservative default position size on error
            return 5.0

    def get_recommendation(self, score, market_type='A', technical_data=None, news_data=None):
        """
        Generate investment recommendation based on score and additional information
        Enhanced with Time-Space Resonance Trading System strategies
        """
        try:
            # 1. Base recommendation logic - Dynamic threshold adjustment based on score
            if score >= 85:
                base_recommendation = '强烈建议买入'
                confidence = 'high'
                action = 'strong_buy'
            elif score >= 70:
                base_recommendation = '建议买入'
                confidence = 'medium_high'
                action = 'buy'
            elif score >= 55:
                base_recommendation = '谨慎买入'
                confidence = 'medium'
                action = 'cautious_buy'
            elif score >= 45:
                base_recommendation = '持观望态度'
                confidence = 'medium'
                action = 'hold'
            elif score >= 30:
                base_recommendation = '谨慎持有'
                confidence = 'medium'
                action = 'cautious_hold'
            elif score >= 15:
                base_recommendation = '建议减仓'
                confidence = 'medium_high'
                action = 'reduce'
            else:
                base_recommendation = '建议卖出'
                confidence = 'high'
                action = 'sell'

            # 2. Consider market characteristics (Dimension 1: Timeframe Nesting)
            market_adjustment = ""
            if market_type == 'US':
                # US market adjustment factors
                if self._is_earnings_season():
                    if confidence == 'high' or confidence == 'medium_high':
                        confidence = 'medium'
                        market_adjustment = "（财报季临近，波动可能加大，建议适当控制仓位）"

            elif market_type == 'HK':
                # HK market adjustment factors
                mainland_sentiment = self._get_mainland_market_sentiment()
                if mainland_sentiment < -0.3 and (action == 'buy' or action == 'strong_buy'):
                    action = 'cautious_buy'
                    confidence = 'medium'
                    market_adjustment = "（受大陆市场情绪影响，建议控制风险）"

            elif market_type == 'A':
                # A-share specific adjustment factors
                if technical_data and 'Volatility' in technical_data:
                    vol = technical_data.get('Volatility', 0)
                    if vol > 4.0 and (action == 'buy' or action == 'strong_buy'):
                        action = 'cautious_buy'
                        confidence = 'medium'
                        market_adjustment = "（市场波动较大，建议分批买入）"

            # 3. Consider market sentiment (Dimension 2: Filtering)
            sentiment_adjustment = ""
            if news_data and 'market_sentiment' in news_data:
                sentiment = news_data.get('market_sentiment', 'neutral')

                if sentiment == 'bullish' and action in ['hold', 'cautious_hold']:
                    action = 'cautious_buy'
                    sentiment_adjustment = "（市场氛围积极，可适当提高仓位）"

                elif sentiment == 'bearish' and action in ['buy', 'cautious_buy']:
                    action = 'hold'
                    sentiment_adjustment = "（市场氛围悲观，建议等待更好买点）"

            # 4. Technical indicators adjustment (Dimension 2: "Peak Detection System")
            technical_adjustment = ""
            if technical_data:
                rsi = technical_data.get('RSI', 50)
                macd_signal = technical_data.get('MACD_signal', 'neutral')

                # RSI overbought/oversold adjustment
                if rsi > 80 and action in ['buy', 'strong_buy']:
                    action = 'hold'
                    technical_adjustment = "（RSI指标显示超买，建议等待回调）"
                elif rsi < 20 and action in ['sell', 'reduce']:
                    action = 'hold'
                    technical_adjustment = "（RSI指标显示超卖，可能存在反弹机会）"

                # MACD signal adjustment
                if macd_signal == 'bullish' and action in ['hold', 'cautious_hold']:
                    action = 'cautious_buy'
                    if not technical_adjustment:
                        technical_adjustment = "（MACD显示买入信号）"
                elif macd_signal == 'bearish' and action in ['cautious_buy', 'buy']:
                    action = 'hold'
                    if not technical_adjustment:
                        technical_adjustment = "（MACD显示卖出信号）"

            # 5. Convert adjusted action to final recommendation
            action_to_recommendation = {
                'strong_buy': '强烈建议买入',
                'buy': '建议买入',
                'cautious_buy': '谨慎买入',
                'hold': '持观望态度',
                'cautious_hold': '谨慎持有',
                'reduce': '建议减仓',
                'sell': '建议卖出'
            }

            final_recommendation = action_to_recommendation.get(action, base_recommendation)

            # 6. Combine all adjustment factors
            adjustments = " ".join(filter(None, [market_adjustment, sentiment_adjustment, technical_adjustment]))

            if adjustments:
                return f"{final_recommendation} {adjustments}"
            else:
                return final_recommendation

        except Exception as e:
            self.logger.error(f"Error generating investment recommendation: {str(e)}")
            # Return safe default recommendation on error
            return "无法提供明确建议，请结合多种因素谨慎决策"

    def check_consecutive_losses(self, trade_history, max_consecutive_losses=3):
        """
        Implement the "Refractory Period Risk Control" - stop trading after consecutive losses

        Parameters:
            trade_history: List of recent trade results (True for profit, False for loss)
            max_consecutive_losses: Maximum allowed consecutive losses

        Returns:
            Boolean: True if trading should be paused, False if trading can continue
        """
        consecutive_losses = 0

        # Count consecutive losses from most recent trades
        for trade in reversed(trade_history):
            if not trade:  # If trade is a loss
                consecutive_losses += 1
            else:
                break  # Break on first profitable trade

        # Return True if we've hit max consecutive losses
        return consecutive_losses >= max_consecutive_losses

    def check_profit_taking(self, current_profit_percent, threshold=20.0):
        """
        Implement profit-taking mechanism when returns exceed threshold
        Part of "Energy Conservation Dimension"

        Parameters:
            current_profit_percent: Current profit percentage
            threshold: Profit percentage threshold for taking profits

        Returns:
            Float: Percentage of position to reduce (0.0-1.0)
        """
        if current_profit_percent >= threshold:
            # If profit exceeds threshold, suggest reducing position by 50%
            return 0.5

        return 0.0  # No position reduction recommended

    def _is_earnings_season(self):
        """检查当前是否处于财报季(辅助函数)"""
        from datetime import datetime
        current_month = datetime.now().month
        # 美股财报季大致在1月、4月、7月和10月
        return current_month in [1, 4, 7, 10]

    def _check_a_share_linkage(self, df, window=20):
        """检查港股与A股的联动性(辅助函数)"""
        # 该函数需要获取对应的A股指数数据
        # 简化版实现:
        try:
            # 获取恒生指数与上证指数的相关系数
            # 实际实现中需要获取真实数据
            correlation = 0.6  # 示例值
            return correlation
        except:
            return 0.5  # 默认中等关联度

    def _get_mainland_market_sentiment(self):
        """获取中国大陆市场情绪(辅助函数)"""
        # 实际实现中需要分析上证指数、北向资金等因素
        try:
            # 简化版实现，返回-1到1之间的值，1表示积极情绪
            sentiment = 0.2  # 示例值
            return sentiment
        except:
            return 0  # 默认中性情绪

    def get_stock_news(self, stock_code, market_type='A', limit=5):
        """
        获取股票相关新闻和实时信息，通过OpenAI API调用news模型获取
        参数:
            stock_code: 股票代码
            market_type: 市场类型 (A/HK/US)
            limit: 返回的新闻条数上限
        返回:
            包含新闻和公告的字典
        """
        try:
            self.logger.info(f"获取股票 {stock_code} 的相关新闻和信息")

            # 缓存键
            cache_key = f"{stock_code}_{market_type}_news"
            if cache_key in self.data_cache and (
                    datetime.now() - self.data_cache[cache_key]['timestamp']).seconds < 3600:
                # 缓存1小时内的数据
                return self.data_cache[cache_key]['data']

            # 获取股票基本信息
            stock_info = self.get_stock_info(stock_code)
            stock_name = stock_info.get('股票名称', '未知')
            industry = stock_info.get('行业', '未知')

            # 构建新闻查询的prompt
            market_name = "A股" if market_type == 'A' else "港股" if market_type == 'HK' else "美股"
            query = f"""请提供以下股票的最新相关新闻和信息:
            股票名称: {stock_name}
            股票代码: {stock_code}
            市场: {market_name}
            行业: {industry}

            请返回以下格式的JSON数据:
            {{
                "news": [
                    {{"title": "新闻标题", "date": "YYYY-MM-DD", "source": "新闻来源", "summary": "新闻摘要"}},
                    ...
                ],
                "announcements": [
                    {{"title": "公告标题", "date": "YYYY-MM-DD", "type": "公告类型"}},
                    ...
                ],
                "industry_news": [
                    {{"title": "行业新闻标题", "date": "YYYY-MM-DD", "summary": "新闻摘要"}},
                    ...
                ],
                "market_sentiment": "市场情绪(bullish/slightly_bullish/neutral/slightly_bearish/bearish)"
            }}

            每个类别最多返回{limit}条。如果无法获取实际新闻，请基于行业知识生成合理的示例数据。
            """

            messages = [{"role": "user", "content": query}]

            # 使用线程和队列添加超时控制
            import queue
            import threading
            import json
            import openai

            result_queue = queue.Queue()

            def call_api():
                try:
                    # 使用OpenAI API调用news模型
                    response = openai.ChatCompletion.create(
                        model=self.news_model,  # 使用news模型
                        messages=messages,
                        temperature=0.7,
                        max_tokens=4000,
                        stream=False,
                        timeout=240
                    )
                    result_queue.put(response)
                except Exception as e:
                    result_queue.put(e)

            # 启动API调用线程
            api_thread = threading.Thread(target=call_api)
            api_thread.daemon = True
            api_thread.start()

            # 等待结果，最多等待20秒
            try:
                result = result_queue.get(timeout=240)

                # 检查结果是否为异常
                if isinstance(result, Exception):
                    self.logger.error(f"获取新闻API调用失败: {str(result)}")
                    raise result

                # 提取回复内容
                content = result["choices"][0]["message"]["content"].strip()

                # 解析JSON
                try:
                    # 尝试直接解析JSON
                    news_data = json.loads(content)
                except json.JSONDecodeError:
                    # 如果直接解析失败，尝试提取JSON部分
                    import re
                    json_match = re.search(r'```json\s*([\s\S]*?)\s*```', content)
                    if json_match:
                        json_str = json_match.group(1)
                        news_data = json.loads(json_str)
                    else:
                        raise ValueError("无法从响应中提取JSON数据")

                # 添加时间戳
                news_data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # 缓存结果
                self.data_cache[cache_key] = {
                    'data': news_data,
                    'timestamp': datetime.now()
                }

                return news_data

            except queue.Empty:
                self.logger.warning("获取新闻API调用超时")
                return {
                    'news': [],
                    'announcements': [],
                    'industry_news': [],
                    'market_sentiment': 'neutral',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            except Exception as e:
                self.logger.error(f"处理新闻数据时出错: {str(e)}")
                return {
                    'news': [],
                    'announcements': [],
                    'industry_news': [],
                    'market_sentiment': 'neutral',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

        except Exception as e:
            self.logger.error(f"获取股票新闻时出错: {str(e)}")
            # 出错时返回空结果
            return {
                'news': [],
                'announcements': [],
                'industry_news': [],
                'market_sentiment': 'neutral',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

    # def get_recommendation(self, score, market_type='A', technical_data=None, news_data=None):
    #     """
    #     根据得分和附加信息给出平滑的投资建议
    #
    #     参数:
    #         score: 股票综合评分 (0-100)
    #         market_type: 市场类型 (A/HK/US)
    #         technical_data: 技术指标数据 (可选)
    #         news_data: 新闻和市场情绪数据 (可选)
    #
    #     返回:
    #         投资建议字符串
    #     """
    #     try:
    #         # 1. 基础建议逻辑 - 基于分数的平滑建议
    #         if score >= 85:
    #             base_recommendation = '强烈建议买入'
    #             confidence = 'high'
    #             action = 'strong_buy'
    #         elif score >= 70:
    #             base_recommendation = '建议买入'
    #             confidence = 'medium_high'
    #             action = 'buy'
    #         elif score >= 55:
    #             base_recommendation = '谨慎买入'
    #             confidence = 'medium'
    #             action = 'cautious_buy'
    #         elif score >= 45:
    #             base_recommendation = '持观望态度'
    #             confidence = 'medium'
    #             action = 'hold'
    #         elif score >= 30:
    #             base_recommendation = '谨慎持有'
    #             confidence = 'medium'
    #             action = 'cautious_hold'
    #         elif score >= 15:
    #             base_recommendation = '建议减仓'
    #             confidence = 'medium_high'
    #             action = 'reduce'
    #         else:
    #             base_recommendation = '建议卖出'
    #             confidence = 'high'
    #             action = 'sell'
    #
    #         # 2. 考虑市场特性
    #         market_adjustment = ""
    #         if market_type == 'US':
    #             # 美股调整因素
    #             if self._is_earnings_season():
    #                 if confidence == 'high' or confidence == 'medium_high':
    #                     confidence = 'medium'
    #                     market_adjustment = "（财报季临近，波动可能加大，建议适当控制仓位）"
    #
    #         elif market_type == 'HK':
    #             # 港股调整因素
    #             mainland_sentiment = self._get_mainland_market_sentiment()
    #             if mainland_sentiment < -0.3 and (action == 'buy' or action == 'strong_buy'):
    #                 action = 'cautious_buy'
    #                 confidence = 'medium'
    #                 market_adjustment = "（受大陆市场情绪影响，建议控制风险）"
    #
    #         elif market_type == 'A':
    #             # A股特有调整因素
    #             if technical_data and 'Volatility' in technical_data:
    #                 vol = technical_data.get('Volatility', 0)
    #                 if vol > 4.0 and (action == 'buy' or action == 'strong_buy'):
    #                     action = 'cautious_buy'
    #                     confidence = 'medium'
    #                     market_adjustment = "（市场波动较大，建议分批买入）"
    #
    #         # 3. 考虑市场情绪
    #         sentiment_adjustment = ""
    #         if news_data and 'market_sentiment' in news_data:
    #             sentiment = news_data.get('market_sentiment', 'neutral')
    #
    #             if sentiment == 'bullish' and action in ['hold', 'cautious_hold']:
    #                 action = 'cautious_buy'
    #                 sentiment_adjustment = "（市场氛围积极，可适当提高仓位）"
    #
    #             elif sentiment == 'bearish' and action in ['buy', 'cautious_buy']:
    #                 action = 'hold'
    #                 sentiment_adjustment = "（市场氛围悲观，建议等待更好买点）"
    #
    #         # 4. 技术指标微调
    #         technical_adjustment = ""
    #         if technical_data:
    #             rsi = technical_data.get('RSI', 50)
    #             macd_signal = technical_data.get('MACD_signal', 'neutral')
    #
    #             # RSI超买超卖调整
    #             if rsi > 80 and action in ['buy', 'strong_buy']:
    #                 action = 'hold'
    #                 technical_adjustment = "（RSI指标显示超买，建议等待回调）"
    #             elif rsi < 20 and action in ['sell', 'reduce']:
    #                 action = 'hold'
    #                 technical_adjustment = "（RSI指标显示超卖，可能存在反弹机会）"
    #
    #             # MACD信号调整
    #             if macd_signal == 'bullish' and action in ['hold', 'cautious_hold']:
    #                 action = 'cautious_buy'
    #                 if not technical_adjustment:
    #                     technical_adjustment = "（MACD显示买入信号）"
    #             elif macd_signal == 'bearish' and action in ['cautious_buy', 'buy']:
    #                 action = 'hold'
    #                 if not technical_adjustment:
    #                     technical_adjustment = "（MACD显示卖出信号）"
    #
    #         # 5. 根据调整后的action转换为最终建议
    #         action_to_recommendation = {
    #             'strong_buy': '强烈建议买入',
    #             'buy': '建议买入',
    #             'cautious_buy': '谨慎买入',
    #             'hold': '持观望态度',
    #             'cautious_hold': '谨慎持有',
    #             'reduce': '建议减仓',
    #             'sell': '建议卖出'
    #         }
    #
    #         final_recommendation = action_to_recommendation.get(action, base_recommendation)
    #
    #         # 6. 组合所有调整因素
    #         adjustments = " ".join(filter(None, [market_adjustment, sentiment_adjustment, technical_adjustment]))
    #
    #         if adjustments:
    #             return f"{final_recommendation} {adjustments}"
    #         else:
    #             return final_recommendation
    #
    #     except Exception as e:
    #         self.logger.error(f"生成投资建议时出错: {str(e)}")
    #         # 出错时返回安全的默认建议
    #         return "无法提供明确建议，请结合多种因素谨慎决策"

    # 原有API：使用 OpenAI 替代 Gemini
    # def get_ai_analysis(self, df, stock_code):
    #     """使用AI进行分析"""
    #     try:
    #         import openai
    #         import threading
    #         import queue

    #         # 设置API密钥和基础URL
    #         openai.api_key = self.openai_api_key
    #         openai.api_base = self.openai_api_url

    #         recent_data = df.tail(14).to_dict('records')
    #         technical_summary = {
    #             'trend': 'upward' if df.iloc[-1]['MA5'] > df.iloc[-1]['MA20'] else 'downward',
    #             'volatility': f"{df.iloc[-1]['Volatility']:.2f}%",
    #             'volume_trend': 'increasing' if df.iloc[-1]['Volume_Ratio'] > 1 else 'decreasing',
    #             'rsi_level': df.iloc[-1]['RSI']
    #         }

    #         prompt = f"""分析股票{stock_code}：
    # 技术指标概要：{technical_summary}
    # 近14日交易数据：{recent_data}

    # 请提供：
    # 1.趋势分析（包含支撑位和压力位）
    # 2.成交量分析及其含义
    # 3.风险评估（包含波动率分析）
    # 4.短期和中期目标价位
    # 5.关键技术位分析
    # 6.具体交易建议（包含止损位）

    # 请基于技术指标和市场动态进行分析,给出具体数据支持。"""

    #         messages = [{"role": "user", "content": prompt}]

    #         # 使用线程和队列添加超时控制
    #         result_queue = queue.Queue()

    #         def call_api():
    #             try:
    #                 response = openai.ChatCompletion.create(
    #                     model=self.openai_model,
    #                     messages=messages,
    #                     temperature=1,
    #                     max_tokens=4000,
    #                     stream = False
    #                 )
    #                 result_queue.put(response)
    #             except Exception as e:
    #                 result_queue.put(e)

    #         # 启动API调用线程
    #         api_thread = threading.Thread(target=call_api)
    #         api_thread.daemon = True
    #         api_thread.start()

    #         # 等待结果，最多等待20秒
    #         try:
    #             result = result_queue.get(timeout=20)

    #             # 检查结果是否为异常
    #             if isinstance(result, Exception):
    #                 raise result

    #             # 提取助理回复
    #             assistant_reply = result["choices"][0]["message"]["content"].strip()
    #             return assistant_reply

    #         except queue.Empty:
    #             return "AI分析超时，无法获取分析结果。请稍后再试。"
    #         except Exception as e:
    #             return f"AI分析过程中发生错误: {str(e)}"

    #     except Exception as e:
    #         self.logger.error(f"AI分析发生错误: {str(e)}")
    #         return "AI分析过程中发生错误，请稍后再试"

    def get_ai_analysis(self, df, stock_code, market_type='A'):
        """
        使用AI进行增强分析
        结合技术指标、实时新闻和行业信息

        参数:
            df: 股票历史数据DataFrame
            stock_code: 股票代码
            market_type: 市场类型(A/HK/US)

        返回:
            AI生成的分析报告文本
        """
        try:
            import openai
            import threading
            import queue

            # 设置API密钥和基础URL
            openai.api_key = self.openai_api_key
            openai.api_base = self.openai_api_url

            # 1. 获取最近K线数据
            recent_data = df.tail(20).to_dict('records')

            # 2. 计算技术指标摘要
            technical_summary = {
                'trend': 'upward' if df.iloc[-1]['MA5'] > df.iloc[-1]['MA20'] else 'downward',
                'volatility': f"{df.iloc[-1]['Volatility']:.2f}%",
                'volume_trend': 'increasing' if df.iloc[-1]['Volume_Ratio'] > 1 else 'decreasing',
                'rsi_level': df.iloc[-1]['RSI'],
                'macd_signal': 'bullish' if df.iloc[-1]['MACD'] > df.iloc[-1]['Signal'] else 'bearish',
                'bb_position': self._calculate_bb_position(df)
            }

            # 3. 获取支撑压力位
            sr_levels = self.identify_support_resistance(df)

            # 4. 获取股票基本信息
            stock_info = self.get_stock_info(stock_code)
            stock_name = stock_info.get('股票名称', '未知')
            industry = stock_info.get('行业', '未知')

            # 5. 获取相关新闻和实时信息 - 整合get_stock_news
            self.logger.info(f"获取 {stock_code} 的相关新闻和市场信息")
            news_data = self.get_stock_news(stock_code, market_type)

            # 6. 评分分解
            score = self.calculate_score(df, market_type)
            score_details = getattr(self, 'score_details', {'total': score})

            # 7. 获取投资建议
            # 传递技术指标和新闻数据给get_recommendation函数
            tech_data = {
                'RSI': technical_summary['rsi_level'],
                'MACD_signal': technical_summary['macd_signal'],
                'Volatility': df.iloc[-1]['Volatility']
            }
            recommendation = self.get_recommendation(score, market_type, tech_data, news_data)

            # 8. 构建更全面的prompt
            prompt = f"""作为专业的股票分析师，请对{stock_name}({stock_code})进行全面分析:

    1. 基本信息:
       - 股票名称: {stock_name}
       - 股票代码: {stock_code}
       - 行业: {industry}
       - 市场类型: {"A股" if market_type == 'A' else "港股" if market_type == 'HK' else "美股"}

    2. 技术指标摘要:
       - 趋势: {technical_summary['trend']}
       - 波动率: {technical_summary['volatility']}
       - 成交量趋势: {technical_summary['volume_trend']}
       - RSI: {technical_summary['rsi_level']:.2f}
       - MACD信号: {technical_summary['macd_signal']}
       - 布林带位置: {technical_summary['bb_position']}

    3. 支撑与压力位:
       - 短期支撑位: {', '.join([str(level) for level in sr_levels['support_levels']['short_term']])}
       - 中期支撑位: {', '.join([str(level) for level in sr_levels['support_levels']['medium_term']])}
       - 短期压力位: {', '.join([str(level) for level in sr_levels['resistance_levels']['short_term']])}
       - 中期压力位: {', '.join([str(level) for level in sr_levels['resistance_levels']['medium_term']])}

    4. 综合评分: {score_details['total']}分
       - 趋势评分: {score_details.get('trend', 0)}
       - 波动率评分: {score_details.get('volatility', 0)}
       - 技术指标评分: {score_details.get('technical', 0)}
       - 成交量评分: {score_details.get('volume', 0)}
       - 动量评分: {score_details.get('momentum', 0)}

    5. 投资建议: {recommendation}

    6. 近期相关新闻:
    {self._format_news_for_prompt(news_data.get('news', []))}

    7. 公司公告:
    {self._format_announcements_for_prompt(news_data.get('announcements', []))}

    8. 行业动态:
    {self._format_news_for_prompt(news_data.get('industry_news', []))}

    9. 市场情绪: {news_data.get('market_sentiment', 'neutral')}

    请提供以下内容:
    1. 技术面分析 - 详细分析价格走势、支撑压力位、主要技术指标的信号
    2. 行业和市场环境 - 结合新闻和行业动态分析公司所处环境
    3. 风险因素 - 识别潜在风险点
    4. 具体交易策略 - 给出明确的买入/卖出建议，包括入场点、止损位和目标价位
    5. 短期(1周)、中期(1-3个月)和长期(半年)展望

    请基于数据给出客观分析，不要过度乐观或悲观。分析应该包含具体数据和百分比，避免模糊表述。
    """

            self.logger.info(f'prompt {prompt}')

            messages = [{"role": "user", "content": prompt}]

            # 使用线程和队列添加超时控制
            result_queue = queue.Queue()

            def call_api():
                try:
                    response = openai.ChatCompletion.create(
                        model=self.openai_model,
                        messages=messages,
                        temperature=0.8,
                        max_tokens=4000,
                        stream=False,
                        timeout=180
                    )
                    self.logger.info(f'response '+str(response))
                    result_queue.put(response)
                except Exception as e:
                    result_queue.put(e)

            # 启动API调用线程
            api_thread = threading.Thread(target=call_api)
            api_thread.daemon = True
            api_thread.start()

            # 等待结果，最多等待30秒
            try:
                result = result_queue.get(timeout=300)

                # 检查结果是否为异常
                if isinstance(result, Exception):
                    raise result

                # 提取助理回复
                assistant_reply = result["choices"][0]["message"]["content"].strip()
                return assistant_reply

            except queue.Empty:
                return "AI分析超时，无法获取分析结果。请稍后再试。"
            except Exception as e:
                return f"AI分析过程中发生错误: {str(e)}"

        except Exception as e:
            self.logger.error(f"AI分析发生错误: {str(e)}")
            return f"AI分析过程中发生错误，请稍后再试。错误信息: {str(e)}"

    def _calculate_bb_position(self, df):
        """计算价格在布林带中的位置"""
        latest = df.iloc[-1]
        bb_width = latest['BB_upper'] - latest['BB_lower']
        if bb_width == 0:
            return "middle"

        position = (latest['close'] - latest['BB_lower']) / bb_width

        if position < 0.2:
            return "near lower band (potential oversold)"
        elif position < 0.4:
            return "below middle band"
        elif position < 0.6:
            return "near middle band"
        elif position < 0.8:
            return "above middle band"
        else:
            return "near upper band (potential overbought)"

    def _format_news_for_prompt(self, news_list):
        """格式化新闻列表为prompt字符串"""
        if not news_list:
            return "   无最新相关新闻"

        formatted = ""
        for i, news in enumerate(news_list[:3]):  # 最多显示3条
            date = news.get('date', '')
            title = news.get('title', '')
            source = news.get('source', '')
            formatted += f"   {i + 1}. [{date}] {title} (来源: {source})\n"

        return formatted

    def _format_announcements_for_prompt(self, announcements):
        """格式化公告列表为prompt字符串"""
        if not announcements:
            return "   无最新公告"

        formatted = ""
        for i, ann in enumerate(announcements[:3]):  # 最多显示3条
            date = ann.get('date', '')
            title = ann.get('title', '')
            type_ = ann.get('type', '')
            formatted += f"   {i + 1}. [{date}] {title} (类型: {type_})\n"

        return formatted

    # 原有API：保持接口不变
    def analyze_stock(self, stock_code, market_type='A'):
        """分析单个股票"""
        try:
            # self.clear_cache(stock_code, market_type)
            # 获取股票数据
            df = self.get_stock_data(stock_code, market_type)
            self.logger.info(f"获取股票数据完成")
            # 计算技术指标
            df = self.calculate_indicators(df)
            self.logger.info(f"计算技术指标完成")
            # 评分系统
            score = self.calculate_score(df)
            self.logger.info(f"评分系统完成")
            # 获取最新数据
            latest = df.iloc[-1]
            prev = df.iloc[-2]

            # 获取基本信息
            stock_info = self.get_stock_info(stock_code)
            stock_name = stock_info.get('股票名称', '未知')
            industry = stock_info.get('行业', '未知')

            # 生成报告（保持原有格式）
            report = {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'industry': industry,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'score': score,
                'price': latest['close'],
                'price_change': (latest['close'] - prev['close']) / prev['close'] * 100,
                'ma_trend': 'UP' if latest['MA5'] > latest['MA20'] else 'DOWN',
                'rsi': latest['RSI'],
                'macd_signal': 'BUY' if latest['MACD'] > latest['Signal'] else 'SELL',
                'volume_status': '放量' if latest['Volume_Ratio'] > 1.5 else '平量',
                'recommendation': self.get_recommendation(score),
                'ai_analysis': self.get_ai_analysis(df, stock_code)
            }

            return report

        except Exception as e:
            self.logger.error(f"分析股票时出错: {str(e)}")
            raise

    # 原有API：保持接口不变
    def scan_market(self, stock_list, min_score=60, market_type='A', force_refresh=False):
        """扫描市场，寻找符合条件的股票（支持缓存）"""
        recommendations = []
        total_stocks = len(stock_list)

        self.logger.info(f"开始市场扫描，共 {total_stocks} 只股票")
        start_time = time.time()
        processed = 0

        # 批量处理，减少日志输出
        batch_size = 10
        for i in range(0, total_stocks, batch_size):
            batch = stock_list[i:i + batch_size]
            batch_results = []

            for stock_code in batch:
                try:
                    # 使用简化版分析以加快速度（传递force_refresh参数）
                    report = self.quick_analyze_stock(stock_code, market_type, force_refresh=force_refresh)
                    if report['score'] >= min_score:
                        batch_results.append(report)
                except Exception as e:
                    self.logger.error(f"分析股票 {stock_code} 时出错: {str(e)}")
                    continue

            # 添加批处理结果
            recommendations.extend(batch_results)

            # 更新处理进度
            processed += len(batch)
            elapsed = time.time() - start_time
            remaining = (elapsed / processed) * (total_stocks - processed) if processed > 0 else 0

            self.logger.info(
                f"已处理 {processed}/{total_stocks} 只股票，耗时 {elapsed:.1f}秒，预计剩余 {remaining:.1f}秒")

        # 按得分排序
        recommendations.sort(key=lambda x: x['score'], reverse=True)

        total_time = time.time() - start_time
        self.logger.info(
            f"市场扫描完成，共分析 {total_stocks} 只股票，找到 {len(recommendations)} 只符合条件的股票，总耗时 {total_time:.1f}秒")

        return recommendations

    # def quick_analyze_stock(self, stock_code, market_type='A'):
    #     """快速分析股票，用于市场扫描"""
    #     try:
    #         # 获取股票数据
    #         df = self.get_stock_data(stock_code, market_type)

    #         # 计算技术指标
    #         df = self.calculate_indicators(df)

    #         # 简化评分计算
    #         score = self.calculate_score(df)

    #         # 获取最新数据
    #         latest = df.iloc[-1]
    #         prev = df.iloc[-2] if len(df) > 1 else latest

    #         # 尝试获取股票名称和行业
    #         try:
    #             stock_info = self.get_stock_info(stock_code)
    #             stock_name = stock_info.get('股票名称', '未知')
    #             industry = stock_info.get('行业', '未知')
    #         except:
    #             stock_name = '未知'
    #             industry = '未知'

    #         # 生成简化报告
    #         report = {
    #             'stock_code': stock_code,
    #             'stock_name': stock_name,
    #             'industry': industry,
    #             'analysis_date': datetime.now().strftime('%Y-%m-%d'),
    #             'score': score,
    #             'price': float(latest['close']),
    #             'price_change': float((latest['close'] - prev['close']) / prev['close'] * 100),
    #             'ma_trend': 'UP' if latest['MA5'] > latest['MA20'] else 'DOWN',
    #             'rsi': float(latest['RSI']),
    #             'macd_signal': 'BUY' if latest['MACD'] > latest['Signal'] else 'SELL',
    #             'volume_status': '放量' if latest['Volume_Ratio'] > 1.5 else '平量',
    #             'recommendation': self.get_recommendation(score)
    #         }

    #         return report
    #     except Exception as e:
    #         self.logger.error(f"快速分析股票 {stock_code} 时出错: {str(e)}")
    #         raise

    def quick_analyze_stock(self, stock_code, market_type='A', start_date=None, end_date=None, force_refresh=False):
        """快速分析股票，用于市场扫描（支持缓存）"""
        try:
            logging.info(f"开始快速分析股票 {stock_code}（强制刷新: {force_refresh}）")
            
            # 如果不是强制刷新，先尝试从缓存获取
            if not force_refresh:
                cached_result = self._get_market_scan_from_cache(stock_code, market_type)
                if cached_result:
                    self.logger.info(f"从缓存获取市场扫描结果: {stock_code}")
                    # 移除缓存时间戳等内部字段
                    result = cached_result.copy()
                    result.pop('cache_time', None)
                    result.pop('from_cache', None)
                    result.pop('cache_remaining', None)
                    return result
            
            # 获取股票数据，支持日期范围
            if start_date or end_date:
                df = self.get_stock_data(stock_code, market_type, start_date=start_date, end_date=end_date)
            else:
                df = self.get_stock_data(stock_code, market_type)

            # 计算技术指标
            df = self.calculate_indicators(df)

            # 简化评分计算
            score = self.calculate_score(df)

            # 获取最新数据
            latest = df.iloc[-1]
            prev = df.iloc[-2] if len(df) > 1 else latest

            # 先获取股票信息再生成报告
            try:
                stock_info = self.get_stock_info(stock_code)
                stock_name = stock_info.get('股票名称', '未知')
                industry = stock_info.get('行业', '未知')

                # 添加日志
                self.logger.info(f"股票 {stock_code} 信息: 名称={stock_name}, 行业={industry}")
            except Exception as e:
                self.logger.error(f"获取股票 {stock_code} 信息时出错: {str(e)}")
                stock_name = '未知'
                industry = '未知'

            # 生成简化报告
            report = {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'industry': industry,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'score': score,
                'price': float(latest['close']),
                'price_change': float((latest['close'] - prev['close']) / prev['close'] * 100),
                'ma_trend': 'UP' if latest['MA5'] > latest['MA20'] else 'DOWN',
                'rsi': float(latest['RSI']),
                'macd_signal': 'BUY' if latest['MACD'] > latest['Signal'] else 'SELL',
                'volume_status': 'HIGH' if latest['Volume_Ratio'] > 1.5 else 'NORMAL',
                'recommendation': self.get_recommendation(score)
            }

            # 保存到缓存
            self._save_market_scan_to_cache(stock_code, report, market_type)

            return report
        except Exception as e:
            self.logger.error(f"快速分析股票 {stock_code} 时出错: {str(e)}")
            raise

    # ======================== 新增功能 ========================#

    def get_stock_info(self, stock_code):
        """获取股票基本信息"""
        import akshare as ak

        cache_key = f"{stock_code}_info"
        if cache_key in self.data_cache:
            return self.data_cache[cache_key]

        try:
            # 获取A股股票基本信息
            stock_info = ak.stock_individual_info_em(symbol=stock_code)

            # 修改：使用列名而不是索引访问数据
            info_dict = {}
            for _, row in stock_info.iterrows():
                # 使用iloc安全地获取数据
                if len(row) >= 2:  # 确保有至少两列
                    info_dict[row.iloc[0]] = row.iloc[1]

            # 获取股票名称
            try:
                stock_name = ak.stock_info_a_code_name()

                # 检查数据框是否包含预期的列
                if '代码' in stock_name.columns and '名称' in stock_name.columns:
                    # 尝试找到匹配的股票代码
                    matched_stocks = stock_name[stock_name['代码'] == stock_code]
                    if not matched_stocks.empty:
                        name = matched_stocks['名称'].values[0]
                    else:
                        self.logger.warning(f"未找到股票代码 {stock_code} 的名称信息")
                        name = "未知"
                else:
                    # 尝试使用不同的列名
                    possible_code_columns = ['代码', 'code', 'symbol', '股票代码', 'stock_code']
                    possible_name_columns = ['名称', 'name', '股票名称', 'stock_name']

                    code_col = next((col for col in possible_code_columns if col in stock_name.columns), None)
                    name_col = next((col for col in possible_name_columns if col in stock_name.columns), None)

                    if code_col and name_col:
                        matched_stocks = stock_name[stock_name[code_col] == stock_code]
                        if not matched_stocks.empty:
                            name = matched_stocks[name_col].values[0]
                        else:
                            name = "未知"
                    else:
                        self.logger.warning(f"股票信息DataFrame结构不符合预期: {stock_name.columns.tolist()}")
                        name = "未知"
            except Exception as e:
                self.logger.error(f"获取股票名称时出错: {str(e)}")
                name = "未知"

            info_dict['股票名称'] = name

            
            # 确保基本字段存在
            if '行业' not in info_dict:
                info_dict['行业'] = "未知"
            if '地区' not in info_dict:
                info_dict['地区'] = "未知"

            # 增加更多日志来调试问题
            self.logger.info(f"获取到股票信息: 代码={stock_code}, 名称={info_dict.get('股票名称', '未知')}, 行业={info_dict.get('行业', '未知')}")

            self.data_cache[cache_key] = info_dict
            return info_dict
        except Exception as e:
            self.logger.error(f"获取股票信息失败: {str(e)}")
            return {"股票名称": "未知", "行业": "未知", "地区": "未知"}

    def identify_support_resistance(self, df):
        """识别支撑位和压力位"""
        latest_price = df['close'].iloc[-1]

        # 使用布林带作为支撑压力参考
        support_levels = [df['BB_lower'].iloc[-1]]
        resistance_levels = [df['BB_upper'].iloc[-1]]

        # 添加主要均线作为支撑压力
        if latest_price < df['MA5'].iloc[-1]:
            resistance_levels.append(df['MA5'].iloc[-1])
        else:
            support_levels.append(df['MA5'].iloc[-1])

        if latest_price < df['MA20'].iloc[-1]:
            resistance_levels.append(df['MA20'].iloc[-1])
        else:
            support_levels.append(df['MA20'].iloc[-1])

        # 添加整数关口
        price_digits = len(str(int(latest_price)))
        base = 10 ** (price_digits - 1)

        lower_integer = math.floor(latest_price / base) * base
        upper_integer = math.ceil(latest_price / base) * base

        if lower_integer < latest_price:
            support_levels.append(lower_integer)
        if upper_integer > latest_price:
            resistance_levels.append(upper_integer)

        # 排序并格式化
        support_levels = sorted(set([round(x, 2) for x in support_levels if x < latest_price]), reverse=True)
        resistance_levels = sorted(set([round(x, 2) for x in resistance_levels if x > latest_price]))

        # 分类为短期和中期
        short_term_support = support_levels[:1] if support_levels else []
        medium_term_support = support_levels[1:2] if len(support_levels) > 1 else []
        short_term_resistance = resistance_levels[:1] if resistance_levels else []
        medium_term_resistance = resistance_levels[1:2] if len(resistance_levels) > 1 else []

        return {
            'support_levels': {
                'short_term': short_term_support,
                'medium_term': medium_term_support
            },
            'resistance_levels': {
                'short_term': short_term_resistance,
                'medium_term': medium_term_resistance
            }
        }

    def calculate_technical_score(self, df):
        """计算技术面评分 (0-40分)"""
        try:
            score = 0
            # 确保有足够的数据
            if len(df) < 2:
                self.logger.warning("数据不足，无法计算技术面评分")
                return {'total': 0, 'trend': 0, 'indicators': 0, 'support_resistance': 0, 'volatility_volume': 0}

            self.logger.info('股票数据长度'+str(len(df)))

            latest = df.iloc[-1]
            prev = df.iloc[-2]  # 获取前一个时间点的数据
            prev_close = prev['close']

            # 1. 趋势分析 (0-10分)
            trend_score = 0

            # 均线排列情况
            if latest['MA5'] > latest['MA20'] > latest['MA60']:  # 多头排列
                trend_score += 5
            elif latest['MA5'] < latest['MA20'] < latest['MA60']:  # 空头排列
                trend_score = 0
            else:  # 交叉状态
                if latest['MA5'] > latest['MA20']:
                    trend_score += 3
                if latest['MA20'] > latest['MA60']:
                    trend_score += 2

            # 价格与均线关系
            if latest['close'] > latest['MA5']:
                trend_score += 3
            elif latest['close'] > latest['MA20']:
                trend_score += 2

            # 限制最大值
            trend_score = min(trend_score, 10)
            score += trend_score

            # 2. 技术指标分析 (0-10分)
            indicator_score = 0

            # RSI
            if 40 <= latest['RSI'] <= 60:  # 中性
                indicator_score += 2
            elif 30 <= latest['RSI'] < 40 or 60 < latest['RSI'] <= 70:  # 边缘区域
                indicator_score += 4
            elif latest['RSI'] < 30:  # 超卖
                indicator_score += 5
            elif latest['RSI'] > 70:  # 超买
                indicator_score += 0

            # MACD
            if latest['MACD'] > latest['Signal']:  # MACD金叉或在零轴上方
                indicator_score += 3
            else:
                # 修复：比较当前和前一个时间点的MACD柱状图值
                if latest['MACD_hist'] > prev['MACD_hist']:  # 柱状图上升
                    indicator_score += 1

            # 限制最大值和最小值
            indicator_score = max(0, min(indicator_score, 10))
            score += indicator_score

            # 3. 支撑压力位分析 (0-10分)
            sr_score = 0

            # 识别支撑位和压力位
            middle_price = latest['close']
            upper_band = latest['BB_upper']
            lower_band = latest['BB_lower']

            # 距离布林带上下轨的距离
            upper_distance = (upper_band - middle_price) / middle_price * 100
            lower_distance = (middle_price - lower_band) / middle_price * 100

            if lower_distance < 2:  # 接近下轨
                sr_score += 5
            elif lower_distance < 5:
                sr_score += 3

            if upper_distance > 5:  # 距上轨较远
                sr_score += 5
            elif upper_distance > 2:
                sr_score += 2

            # 限制最大值
            sr_score = min(sr_score, 10)
            score += sr_score

            # 4. 波动性和成交量分析 (0-10分)
            vol_score = 0

            # 波动率分析
            if latest['Volatility'] < 2:  # 低波动率
                vol_score += 3
            elif latest['Volatility'] < 4:  # 中等波动率
                vol_score += 2

            # 成交量分析
            if 'Volume_Ratio' in df.columns:
                if latest['Volume_Ratio'] > 1.5 and latest['close'] > prev_close:  # 放量上涨
                    vol_score += 4
                elif latest['Volume_Ratio'] < 0.8 and latest['close'] < prev_close:  # 缩量下跌
                    vol_score += 3
                elif latest['Volume_Ratio'] > 1 and latest['close'] > prev_close:  # 普通放量上涨
                    vol_score += 2

            # 限制最大值
            vol_score = min(vol_score, 10)
            score += vol_score

            # 保存各个维度的分数
            technical_scores = {
                'total': score,
                'trend': trend_score,
                'indicators': indicator_score,
                'support_resistance': sr_score,
                'volatility_volume': vol_score
            }

            return technical_scores

        except Exception as e:
            self.logger.error(f"计算技术面评分时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")
            return {'total': 0, 'trend': 0, 'indicators': 0, 'support_resistance': 0, 'volatility_volume': 0}

    def perform_enhanced_analysis(self, stock_code, market_type='A'):
        """执行增强版分析"""
        try:
            # 记录开始时间，便于性能分析
            start_time = time.time()
            self.logger.info(f"开始执行股票 {stock_code} 的增强分析")

            # 获取股票数据
            df = self.get_stock_data(stock_code, market_type)
            data_time = time.time()
            self.logger.info(f"获取股票数据耗时: {data_time - start_time:.2f}秒")

            # 计算技术指标
            df = self.calculate_indicators(df)
            indicator_time = time.time()
            self.logger.info(f"计算技术指标耗时: {indicator_time - data_time:.2f}秒")

            # 获取最新数据
            latest = df.iloc[-1]
            prev = df.iloc[-2] if len(df) > 1 else latest

            # 获取支撑压力位
            sr_levels = self.identify_support_resistance(df)

            # 计算综合评分
            comprehensive_score = self.calculate_score(df, market_type)
            
            # 获取股票信息
            stock_info = self.get_stock_info(stock_code)

            # 获取详细的子评分（从score_details获取）
            if hasattr(self, 'score_details') and self.score_details:
                technical_score = {
                    'total': comprehensive_score,
                    'trend': self.score_details.get('trend', 0),
                    'technical': self.score_details.get('technical', 0),
                    'volatility': self.score_details.get('volatility', 0),
                    'volume': self.score_details.get('volume', 0),
                    'momentum': self.score_details.get('momentum', 0)
                }
            else:
                technical_score = {'total': comprehensive_score}

            # 生成增强版报告
            enhanced_report = {
                'basic_info': {
                    'stock_code': stock_code,
                    'stock_name': stock_info.get('股票名称', '未知'),
                    'industry': stock_info.get('行业', '未知'),
                    'analysis_date': datetime.now().strftime('%Y-%m-%d')
                },
                'price_data': {
                    'current_price': float(latest['close']),  # 确保是Python原生类型
                    'price_change': float((latest['close'] - prev['close']) / prev['close'] * 100),
                    'price_change_value': float(latest['close'] - prev['close'])
                },
                'technical_analysis': {
                    'trend': {
                        'ma_trend': 'UP' if latest['MA5'] > latest['MA20'] else 'DOWN',
                        'ma_status': "多头排列" if latest['MA5'] > latest['MA20'] > latest['MA60'] else
                        "空头排列" if latest['MA5'] < latest['MA20'] < latest['MA60'] else
                        "交叉状态",
                        'ma_values': {
                            'ma5': float(latest['MA5']),
                            'ma20': float(latest['MA20']),
                            'ma60': float(latest['MA60'])
                        }
                    },
                    'indicators': {
                        # 确保所有指标都存在并是原生类型
                        'rsi': float(latest['RSI']) if 'RSI' in latest else 50.0,
                        'macd': float(latest['MACD']) if 'MACD' in latest else 0.0,
                        'macd_signal': float(latest['Signal']) if 'Signal' in latest else 0.0,
                        'macd_histogram': float(latest['MACD_hist']) if 'MACD_hist' in latest else 0.0,
                        'volatility': float(latest['Volatility']) if 'Volatility' in latest else 0.0
                    },
                    'volume': {
                        'current_volume': float(latest['volume']) if 'volume' in latest else 0.0,
                        'volume_ratio': float(latest['Volume_Ratio']) if 'Volume_Ratio' in latest else 1.0,
                        'volume_status': '放量' if 'Volume_Ratio' in latest and latest['Volume_Ratio'] > 1.5 else '平量'
                    },
                    'support_resistance': sr_levels
                },
                'scores': technical_score,
                'recommendation': {
                    'action': self.get_recommendation(technical_score['total']),
                    'key_points': []
                },
                'ai_analysis': self.get_ai_analysis(df, stock_code)
            }

            # 最后检查并修复报告结构
            self._validate_and_fix_report(enhanced_report)

            # 在函数结束时记录总耗时
            end_time = time.time()
            self.logger.info(f"执行增强分析总耗时: {end_time - start_time:.2f}秒")

            return enhanced_report

        except Exception as e:
            self.logger.error(f"执行增强版分析时出错: {str(e)}")
            self.logger.error(traceback.format_exc())

            # 返回基础错误报告
            return {
                'basic_info': {
                    'stock_code': stock_code,
                    'stock_name': '分析失败',
                    'industry': '未知',
                    'analysis_date': datetime.now().strftime('%Y-%m-%d')
                },
                'price_data': {
                    'current_price': 0.0,
                    'price_change': 0.0,
                    'price_change_value': 0.0
                },
                'technical_analysis': {
                    'trend': {
                        'ma_trend': 'UNKNOWN',
                        'ma_status': '未知',
                        'ma_values': {'ma5': 0.0, 'ma20': 0.0, 'ma60': 0.0}
                    },
                    'indicators': {
                        'rsi': 50.0,
                        'macd': 0.0,
                        'macd_signal': 0.0,
                        'macd_histogram': 0.0,
                        'volatility': 0.0
                    },
                    'volume': {
                        'current_volume': 0.0,
                        'volume_ratio': 0.0,
                        'volume_status': 'NORMAL'
                    },
                    'support_resistance': {
                        'support_levels': {'short_term': [], 'medium_term': []},
                        'resistance_levels': {'short_term': [], 'medium_term': []}
                    }
                },
                'scores': {'total': 0},
                'recommendation': {'action': '分析出错，无法提供建议'},
                'ai_analysis': f"分析过程中出错: {str(e)}"
            }

            return error_report

    # 添加一个辅助方法确保报告结构完整
    def _validate_and_fix_report(self, report):
        """确保分析报告结构完整"""
        # 检查必要的顶级字段
        required_sections = ['basic_info', 'price_data', 'technical_analysis', 'scores', 'recommendation',
                             'ai_analysis']
        for section in required_sections:
            if section not in report:
                self.logger.warning(f"报告缺少 {section} 部分，添加空对象")
                report[section] = {}

        # 检查technical_analysis的结构
        if 'technical_analysis' in report:
            tech = report['technical_analysis']
            if not isinstance(tech, dict):
                report['technical_analysis'] = {}
                tech = report['technical_analysis']

            # 检查indicators部分
            if 'indicators' not in tech or not isinstance(tech['indicators'], dict):
                tech['indicators'] = {
                    'rsi': 50.0,
                    'macd': 0.0,
                    'macd_signal': 0.0,
                    'macd_histogram': 0.0,
                    'volatility': 0.0
                }

            # 转换所有指标为原生Python类型
            for key, value in tech['indicators'].items():
                try:
                    tech['indicators'][key] = float(value)
                except (TypeError, ValueError):
                    tech['indicators'][key] = 0.0

    def calculate_can_slim_score(self, stock_code: str, market_type: str = 'A', force_refresh: bool = False) -> dict:
        """
        计算CAN SLIM（欧奈尔）评分
        
        Args:
            stock_code: 股票代码
            market_type: 市场类型
            force_refresh: 是否强制刷新（忽略缓存）
            
        Returns:
            dict: CAN SLIM各项评分和总分
        """
        try:
            self.logger.info(f"开始计算 {stock_code} 的CAN SLIM评分（强制刷新: {force_refresh}）")
            
            # 如果不是强制刷新，先尝试从缓存获取
            if not force_refresh:
                cached_result = self._get_can_slim_from_cache(stock_code, market_type)
                if cached_result:
                    return cached_result
            
            # 获取基本信息
            basic_info = self._get_stock_basic_info(stock_code)
            if not basic_info:
                return self._get_default_can_slim_score("获取股票基本信息失败")
            
            # 获取历史数据（一年期）
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            
            hist_data = self.get_stock_data(stock_code, market_type, start_date, end_date)
            if hist_data.empty:
                return self._simplified_can_slim_analysis(basic_info)
            
            # 计算各项CAN SLIM指标
            c_score = self._calculate_c_score(basic_info, hist_data)
            a_score = self._calculate_a_score(hist_data)
            n_score = self._calculate_n_score(hist_data)
            s_score = self._calculate_s_score(hist_data)
            l_score = self._calculate_l_score(hist_data, stock_code)
            i_score = self._calculate_i_score(basic_info)
            m_score = self._calculate_m_score()
            
            total_score = (c_score + a_score + n_score + s_score + l_score + i_score + m_score) / 7
            
            # 获取投资建议
            recommendation = self._get_can_slim_recommendation(total_score)
            
            result = {
                "c_score": round(c_score, 1),           # 当前季度收益
                "a_score": round(a_score, 1),           # 年度收益增长
                "n_score": round(n_score, 1),           # 新高突破
                "s_score": round(s_score, 1),           # 供需关系
                "l_score": round(l_score, 1),           # 相对强度
                "i_score": round(i_score, 1),           # 机构持股
                "m_score": round(m_score, 1),           # 市场趋势
                "total_score": round(total_score, 1),    # 总分
                "recommendation": recommendation,         # 投资建议
                "basic_info": basic_info,               # 基本信息
                "analysis_time": datetime.now().isoformat()
            }
            
            # 获取AI分析建议
            try:
                ai_analysis = self._get_ai_analysis_for_can_slim(result, stock_code)
                result["ai_analysis"] = ai_analysis
                self.logger.info(f"{stock_code} AI分析完成")
            except Exception as e:
                self.logger.error(f"获取 {stock_code} AI分析失败: {str(e)}")
                result["ai_analysis"] = "AI分析暂时不可用，请稍后再试"
            
            self.logger.info(f"{stock_code} CAN SLIM评分计算完成: 总分 {total_score:.1f}")
            
            # 保存到缓存
            self._save_can_slim_to_cache(stock_code, market_type, result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"计算 {stock_code} CAN SLIM评分失败: {str(e)}")
            return self._get_default_can_slim_score(f"计算失败: {str(e)}")

    def _get_stock_basic_info(self, stock_code: str) -> dict:
        """获取股票基本信息"""
        try:
            stock_data = ak.stock_zh_a_spot_em()
            stock_info = stock_data[stock_data['代码'] == stock_code]
            
            if stock_info.empty:
                return None
            
            row = stock_info.iloc[0]
            return {
                "代码": stock_code,
                "名称": row['名称'],
                "当前价格": float(row['最新价']),
                "涨跌幅": float(row['涨跌幅']),
                "市盈率": float(row.get('市盈率-动态', 0)),
                "总市值": float(row.get('总市值', 0)) / 100000000,  # 转为亿元
                "成交量": float(row['成交量']),
                "成交额": float(row.get('成交额', 0))
            }
        except Exception as e:
            self.logger.error(f"获取股票基本信息失败: {str(e)}")
            return None

    def _calculate_c_score(self, basic_info: dict, hist_data: pd.DataFrame) -> float:
        """当前收益评分 - Current Earnings"""
        try:
            # 基于近期涨跌幅和成交量
            recent_change = basic_info["涨跌幅"]
            
            # 获取近期价格变化趋势
            if len(hist_data) >= 5:
                recent_prices = hist_data['close'].tail(5)
                price_trend = (recent_prices.iloc[-1] / recent_prices.iloc[0] - 1) * 100
                
                # 结合短期趋势调整评分
                if recent_change >= 5 and price_trend > 3:
                    return 90
                elif recent_change >= 2 and price_trend > 1:
                    return 75
                elif recent_change >= 0:
                    return 60
                elif recent_change >= -2:
                    return 45
                else:
                    return 25
            else:
                # 简化评分
                if recent_change >= 5: return 85
                elif recent_change >= 2: return 70
                elif recent_change >= 0: return 60
                elif recent_change >= -2: return 45
                else: return 25
                
        except Exception:
            return 50

    def _calculate_a_score(self, hist_data: pd.DataFrame) -> float:
        """年度收益增长评分 - Annual Earnings Growth"""
        try:
            if len(hist_data) >= 200:  # 确保有足够的一年数据
                year_ago_price = hist_data['close'].iloc[0]
                current_price = hist_data['close'].iloc[-1]
                annual_return = (current_price / year_ago_price - 1) * 100
                
                if annual_return >= 50: return 95
                elif annual_return >= 30: return 85
                elif annual_return >= 20: return 70
                elif annual_return >= 10: return 60
                elif annual_return >= 0: return 50
                else: return max(20, 40 + annual_return * 0.5)
            return 50
        except Exception:
            return 50

    def _calculate_n_score(self, hist_data: pd.DataFrame) -> float:
        """新高评分 - New Price Highs"""
        try:
            current_price = hist_data['close'].iloc[-1]
            high_52week = hist_data['high'].max()
            
            # 计算距离52周高点的百分比
            distance_from_high = (current_price / high_52week) * 100
            
            if distance_from_high >= 98: return 95
            elif distance_from_high >= 90: return 85
            elif distance_from_high >= 80: return 70
            elif distance_from_high >= 70: return 60
            else: return max(25, distance_from_high * 0.7)
        except Exception:
            return 50

    def _calculate_s_score(self, hist_data: pd.DataFrame) -> float:
        """供需关系评分 - Supply and Demand"""
        try:
            # 计算成交量趋势
            recent_volume = hist_data['vol'].tail(10).mean()
            avg_volume = hist_data['vol'].mean()
            volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1
            
            # 计算价格变化
            price_change_10d = (hist_data['close'].iloc[-1] / hist_data['close'].iloc[-10] - 1) * 100 if len(hist_data) >= 10 else 0
            
            # 综合评分
            if volume_ratio > 2.0 and price_change_10d > 5:
                return 95
            elif volume_ratio > 1.5 and price_change_10d > 2:
                return 80
            elif volume_ratio > 1.2 and price_change_10d > 0:
                return 65
            elif price_change_10d > -2:
                return 50
            else:
                return 30
        except Exception:
            return 50

    def _calculate_l_score(self, hist_data: pd.DataFrame, stock_code: str) -> float:
        """相对强度评分 - Leader or Laggard"""
        try:
            # 获取市场基准（上证指数）数据进行比较
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            
            # 尝试获取上证指数数据
            try:
                market_data = ak.stock_zh_index_daily(symbol="sh000001")
                market_data = market_data[(market_data['date'] >= start_date) & (market_data['date'] <= end_date)]
                
                if not market_data.empty and len(hist_data) >= 200:
                    # 计算股票和市场的收益率
                    stock_return = (hist_data['close'].iloc[-1] / hist_data['close'].iloc[0] - 1) * 100
                    market_return = (market_data['close'].iloc[-1] / market_data['close'].iloc[0] - 1) * 100
                    
                    # 相对强度
                    relative_strength = stock_return - market_return
                    
                    if relative_strength >= 30: return 90
                    elif relative_strength >= 20: return 80
                    elif relative_strength >= 10: return 70
                    elif relative_strength >= 0: return 60
                    elif relative_strength >= -10: return 45
                    else: return 30
            except Exception:
                pass
            
            # 如果无法获取市场数据，基于股票自身表现
            if len(hist_data) >= 200:
                stock_return = (hist_data['close'].iloc[-1] / hist_data['close'].iloc[0] - 1) * 100
                if stock_return >= 20: return 75
                elif stock_return >= 10: return 65
                elif stock_return >= 0: return 55
                else: return 40
            
            return 50
        except Exception:
            return 50

    def _calculate_i_score(self, basic_info: dict) -> float:
        """机构持股评分 - Institutional Sponsorship"""
        try:
            market_cap = basic_info["总市值"]
            turnover = basic_info.get("成交额", 0)
            
            # 基于市值和流动性评估机构关注度
            if market_cap >= 1000:  # 千亿以上大盘股
                return 85
            elif market_cap >= 500:  # 500-1000亿
                return 75
            elif market_cap >= 200:  # 200-500亿
                return 65
            elif market_cap >= 100:  # 100-200亿
                return 55
            elif market_cap >= 50:   # 50-100亿
                return 45
            else:
                return 30
        except Exception:
            return 50

    def _calculate_m_score(self) -> float:
        """市场趋势评分 - Market Direction"""
        try:
            # 获取上证指数近期数据
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            
            try:
                market_data = ak.stock_zh_index_daily(symbol="sh000001")
                market_data = market_data[(market_data['date'] >= start_date) & (market_data['date'] <= end_date)]
                
                if not market_data.empty and len(market_data) >= 20:
                    # 计算20日均线
                    market_data['MA20'] = market_data['close'].rolling(window=20).mean()
                    current_index = market_data['close'].iloc[-1]
                    ma20 = market_data['MA20'].iloc[-1]
                    
                    # 市场趋势评分
                    if current_index > ma20 * 1.05:  # 强势上涨
                        return 80
                    elif current_index > ma20:       # 温和上涨
                        return 65
                    elif current_index > ma20 * 0.95: # 横盘
                        return 50
                    else:                           # 下跌
                        return 35
            except Exception:
                pass
            
            return 55  # 中性评分
        except Exception:
            return 50

    def _simplified_can_slim_analysis(self, basic_info: dict) -> dict:
        """简化的CAN SLIM分析（当无法获取完整历史数据时）"""
        try:
            market_cap = basic_info["总市值"]
            price_change = basic_info["涨跌幅"]
            pe_ratio = basic_info["市盈率"]
            
            # 基于有限信息的简化评分
            c_score = max(30, min(80, 50 + price_change * 8))
            a_score = 50  # 默认中等
            n_score = max(30, min(70, 50 + price_change * 5))
            s_score = max(30, min(70, 50 + price_change * 3))
            l_score = 50  # 默认中等
            i_score = min(80, max(20, market_cap / 10))
            m_score = 55  # 市场中性
            
            total_score = (c_score + a_score + n_score + s_score + l_score + i_score + m_score) / 7
            recommendation = self._get_can_slim_recommendation(total_score)
            
            result = {
                "c_score": round(c_score, 1),
                "a_score": round(a_score, 1),
                "n_score": round(n_score, 1),
                "s_score": round(s_score, 1),
                "l_score": round(l_score, 1),
                "i_score": round(i_score, 1),
                "m_score": round(m_score, 1),
                "total_score": round(total_score, 1),
                "recommendation": recommendation,
                "basic_info": basic_info,
                "analysis_time": datetime.now().isoformat(),
                "note": "简化分析：历史数据不足，评分可能不够准确"
            }
            
            # 获取AI分析建议（简化分析）
            try:
                ai_analysis = self._get_ai_analysis_for_can_slim(result, basic_info.get("代码", "未知"))
                result["ai_analysis"] = ai_analysis + "\n\n注：基于简化数据分析，建议结合更多信息做出投资决策。"
            except Exception as e:
                result["ai_analysis"] = "简化分析模式下，AI分析功能受限。建议获取完整历史数据后重新分析。"
            
            return result
        except Exception as e:
            return self._get_default_can_slim_score(f"简化分析失败: {str(e)}")

    def _get_can_slim_recommendation(self, total_score: float) -> dict:
        """根据总分获取投资建议"""
        if total_score >= 80:
            return {
                "level": "强烈推荐",
                "action": "买入",
                "risk": "低风险",
                "icon": "🟢",
                "description": "CAN SLIM各项指标表现优异，符合欧奈尔成长股标准"
            }
        elif total_score >= 70:
            return {
                "level": "推荐",
                "action": "关注买入",
                "risk": "中低风险",
                "icon": "🔵",
                "description": "多数指标表现良好，值得重点关注"
            }
        elif total_score >= 60:
            return {
                "level": "谨慎关注",
                "action": "观望",
                "risk": "中等风险",
                "icon": "🟡",
                "description": "部分指标符合要求，需密切关注发展"
            }
        elif total_score >= 50:
            return {
                "level": "观望",
                "action": "暂不建议",
                "risk": "中高风险",
                "icon": "🟠",
                "description": "指标表现一般，建议继续观察"
            }
        else:
            return {
                "level": "不推荐",
                "action": "避开",
                "risk": "高风险",
                "icon": "🔴",
                "description": "多项指标不符合CAN SLIM标准，风险较高"
            }

    def _get_default_can_slim_score(self, error_msg: str = "") -> dict:
        """获取默认的CAN SLIM评分（当计算失败时）"""
        return {
            "c_score": 50.0,
            "a_score": 50.0,
            "n_score": 50.0,
            "s_score": 50.0,
            "l_score": 50.0,
            "i_score": 50.0,
            "m_score": 50.0,
            "total_score": 50.0,
            "recommendation": {
                "level": "数据不足",
                "action": "无法评估",
                "risk": "未知风险",
                "icon": "⚪",
                "description": f"无法计算CAN SLIM评分：{error_msg}"
            },
            "basic_info": None,
            "analysis_time": datetime.now().isoformat(),
            "error": error_msg,
            "ai_analysis": "数据不足，无法提供AI分析建议"
        }

    def _get_ai_analysis_for_can_slim(self, can_slim_result: dict, stock_code: str) -> str:
        """
        基于CAN SLIM评分获取AI分析建议
        
        Args:
            can_slim_result: CAN SLIM评分结果
            stock_code: 股票代码
            
        Returns:
            str: AI分析建议
        """
        try:
            # 构建AI分析的prompt
            scores = {
                "C(当前收益)": can_slim_result.get("c_score", 0),
                "A(年度增长)": can_slim_result.get("a_score", 0),
                "N(新高突破)": can_slim_result.get("n_score", 0),
                "S(供需关系)": can_slim_result.get("s_score", 0),
                "L(相对强度)": can_slim_result.get("l_score", 0),
                "I(机构持股)": can_slim_result.get("i_score", 0),
                "M(市场趋势)": can_slim_result.get("m_score", 0)
            }
            
            total_score = can_slim_result.get("total_score", 0)
            basic_info = can_slim_result.get("basic_info", {})
            stock_name = basic_info.get("名称", stock_code) if basic_info else stock_code
            current_price = basic_info.get("当前价格", 0) if basic_info else 0
            
            prompt = f"""
作为专业的股票投资顾问，请基于以下CAN SLIM（欧奈尔方法学）评分分析，为股票 {stock_name}（{stock_code}）提供详细的投资建议：

股票基本信息：
- 股票名称：{stock_name}
- 股票代码：{stock_code}
- 当前价格：{current_price}元

CAN SLIM各项评分（满分100）：
- C (当前季度收益)：{scores["C(当前收益)"]}分
- A (年度收益增长)：{scores["A(年度增长)"]}分  
- N (新高突破)：{scores["N(新高突破)"]}分
- S (供需关系)：{scores["S(供需关系)"]}分
- L (相对强度)：{scores["L(相对强度)"]}分
- I (机构持股)：{scores["I(机构持股)"]}分
- M (市场趋势)：{scores["M(市场趋势)"]}分

综合评分：{total_score}分

请提供以下内容的分析建议：
1. 对各项评分的解读，指出强项和弱项
2. 基于CAN SLIM方法学的投资建议（买入/持有/卖出）
3. 风险提示和注意事项
4. 具体的操作建议和时机选择
5. 预期目标价位区间（如果适用）

请用简洁专业的语言，控制在300-500字内。
"""

            # 调用AI分析
            return self._call_ai_analysis(prompt)
            
        except Exception as e:
            self.logger.error(f"获取CAN SLIM AI分析失败: {str(e)}")
            return f"AI分析暂不可用：{str(e)}"

    def _call_ai_analysis(self, prompt: str) -> str:
        """
        调用AI模型进行分析
        
        Args:
            prompt: 分析提示词
            
        Returns:
            str: AI分析结果
        """
        try:
            # 这里可以接入不同的AI模型API
            # 例如：OpenAI GPT、文心一言、通义千问等
            
            # 模拟AI分析（实际项目中需要替换为真实的AI API调用）
            analysis = self._simulate_ai_analysis(prompt)
            return analysis
            
        except Exception as e:
            self.logger.error(f"AI模型调用失败: {str(e)}")
            return "AI分析服务暂时不可用，请稍后再试"

    def _simulate_ai_analysis(self, prompt: str) -> str:
        """
        模拟AI分析（实际项目中应替换为真实的AI API调用）
        
        Args:
            prompt: 分析提示词
            
        Returns:
            str: 模拟的AI分析结果
        """
        try:
            # 从prompt中提取评分信息进行简单分析
            import re
            
            # 提取各项评分
            scores_pattern = r'- ([CANSLIM]) \([^)]+\)：(\d+(?:\.\d+)?)分'
            scores_matches = re.findall(scores_pattern, prompt)
            total_score_match = re.search(r'综合评分：(\d+(?:\.\d+)?)分', prompt)
            
            if not scores_matches or not total_score_match:
                return "基于提供的CAN SLIM评分数据，建议谨慎投资并密切关注市场变化。"
            
            total_score = float(total_score_match.group(1))
            scores_dict = {item[0]: float(item[1]) for item in scores_matches}
            
            # 基于评分生成分析
            analysis_parts = []
            
            # 1. 评分解读
            strong_points = [k for k, v in scores_dict.items() if v >= 70]
            weak_points = [k for k, v in scores_dict.items() if v < 50]
            
            if strong_points:
                analysis_parts.append(f"**优势方面**：{self._get_score_interpretation(strong_points)}表现突出，显示了良好的投资潜力。")
            
            if weak_points:
                analysis_parts.append(f"**关注领域**：{self._get_score_interpretation(weak_points)}相对较弱，需要密切关注。")
            
            # 2. 投资建议
            if total_score >= 80:
                advice = "**投资建议**：综合评分优秀，符合欧奈尔成长股投资标准，建议**积极关注**或适量买入，但需注意仓位管理。"
            elif total_score >= 70:
                advice = "**投资建议**：综合评分良好，具备一定投资价值，建议**谨慎买入**，可分批建仓。"
            elif total_score >= 60:
                advice = "**投资建议**：综合评分中等，建议**持续观察**，等待更好的买入时机。"
            else:
                advice = "**投资建议**：综合评分偏低，当前**不建议买入**，建议等待基本面改善。"
            
            analysis_parts.append(advice)
            
            # 3. 风险提示
            risk_warnings = []
            if scores_dict.get('M', 50) < 60:
                risk_warnings.append("市场趋势不明朗")
            if scores_dict.get('S', 50) < 60:
                risk_warnings.append("供需关系偏弱")
            if scores_dict.get('L', 50) < 50:
                risk_warnings.append("相对强度不足")
            
            if risk_warnings:
                analysis_parts.append(f"**风险提示**：需注意{', '.join(risk_warnings)}，建议制定止损策略。")
            
            # 4. 操作建议
            if total_score >= 75:
                operation = "**操作建议**：可考虑在股价回调至关键支撑位时分批买入，设置合理的止损位。"
            elif total_score >= 60:
                operation = "**操作建议**：保持观望，关注基本面变化和技术面突破信号。"
            else:
                operation = "**操作建议**：暂时规避，关注其他更具潜力的投资标的。"
            
            analysis_parts.append(operation)
            
            return "\n\n".join(analysis_parts)
            
        except Exception as e:
            self.logger.error(f"模拟AI分析失败: {str(e)}")
            return "基于CAN SLIM评分分析，建议结合个人风险偏好和市场环境做出投资决策，并严格执行风险管理策略。"

    def _get_score_interpretation(self, score_keys: list) -> str:
        """获取评分项目的中文解释"""
        interpretations = {
            'C': '当前收益',
            'A': '年度增长',
            'N': '新高突破',
            'S': '供需关系',
            'L': '相对强度',
            'I': '机构持股',
            'M': '市场趋势'
        }
        return '、'.join([interpretations.get(key, key) for key in score_keys])

    def _load_can_slim_cache(self):
        """加载CAN SLIM缓存从文件"""
        try:
            if os.path.exists(self.can_slim_cache_file):
                with open(self.can_slim_cache_file, 'r', encoding='utf-8') as f:
                    self.can_slim_cache = json.load(f)
                self.logger.info("CAN SLIM缓存加载成功")
            else:
                self.can_slim_cache = {}
        except Exception as e:
            self.logger.error(f"加载CAN SLIM缓存失败: {str(e)}")
            self.can_slim_cache = {}

    def _save_can_slim_cache(self):
        """保存CAN SLIM缓存到文件"""
        try:
            with open(self.can_slim_cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.can_slim_cache, f, ensure_ascii=False, indent=2)
            self.logger.info("CAN SLIM缓存保存成功")
        except Exception as e:
            self.logger.error(f"保存CAN SLIM缓存失败: {str(e)}")

    def _get_cache_key(self, stock_code: str, market_type: str) -> str:
        """生成缓存键"""
        return f"{stock_code}_{market_type}"

    def _is_cache_valid(self, cache_entry: dict) -> bool:
        """检查缓存是否有效（未过期）"""
        try:
            cache_time = cache_entry.get('cache_time', 0)
            current_time = time.time()
            return (current_time - cache_time) < self.can_slim_cache_duration
        except Exception:
            return False

    def _get_can_slim_from_cache(self, stock_code: str, market_type: str) -> dict:
        """从缓存获取CAN SLIM评分"""
        try:
            # 先加载最新缓存
            self._load_can_slim_cache()
            
            cache_key = self._get_cache_key(stock_code, market_type)
            
            if cache_key in self.can_slim_cache:
                cache_entry = self.can_slim_cache[cache_key]
                
                if self._is_cache_valid(cache_entry):
                    self.logger.info(f"从缓存获取CAN SLIM评分: {stock_code}")
                    result = cache_entry.copy()
                    result['from_cache'] = True
                    result['cache_remaining'] = self.can_slim_cache_duration - (time.time() - cache_entry.get('cache_time', 0))
                    return result
                else:
                    # 缓存过期，删除
                    del self.can_slim_cache[cache_key]
                    self._save_can_slim_cache()
                    
            return None
        except Exception as e:
            self.logger.error(f"获取CAN SLIM缓存失败: {str(e)}")
            return None

    def _save_can_slim_to_cache(self, stock_code: str, market_type: str, result: dict):
        """保存CAN SLIM评分到缓存"""
        try:
            cache_key = self._get_cache_key(stock_code, market_type)
            
            # 添加缓存时间戳
            cached_result = result.copy()
            cached_result['cache_time'] = time.time()
            cached_result['from_cache'] = False
            
            self.can_slim_cache[cache_key] = cached_result
            self._save_can_slim_cache()
            
            self.logger.info(f"CAN SLIM评分已缓存: {stock_code}")
        except Exception as e:
            self.logger.error(f"保存CAN SLIM缓存失败: {str(e)}")

    def clear_can_slim_cache(self, stock_code: str = None, market_type: str = None):
        """清除CAN SLIM缓存"""
        try:
            if stock_code and market_type:
                # 清除特定股票的缓存
                cache_key = self._get_cache_key(stock_code, market_type)
                if cache_key in self.can_slim_cache:
                    del self.can_slim_cache[cache_key]
                    self.logger.info(f"已清除 {stock_code} 的CAN SLIM缓存")
            else:
                # 清除所有缓存
                self.can_slim_cache = {}
                self.logger.info("已清除所有CAN SLIM缓存")
                
            self._save_can_slim_cache()
            return True
        except Exception as e:
            self.logger.error(f"清除CAN SLIM缓存失败: {str(e)}")
            return False

    def get_can_slim_cache_info(self, stock_code: str = None, market_type: str = None) -> dict:
        """获取缓存信息"""
        try:
            self._load_can_slim_cache()
            
            if stock_code and market_type:
                # 获取特定股票的缓存信息
                cache_key = self._get_cache_key(stock_code, market_type)
                if cache_key in self.can_slim_cache:
                    cache_entry = self.can_slim_cache[cache_key]
                    cache_time = cache_entry.get('cache_time', 0)
                    remaining_time = self.can_slim_cache_duration - (time.time() - cache_time)
                    
                    return {
                        'has_cache': True,
                        'is_valid': self._is_cache_valid(cache_entry),
                        'cache_time': datetime.fromtimestamp(cache_time).isoformat(),
                        'remaining_minutes': max(0, int(remaining_time / 60)),
                        'total_duration_minutes': int(self.can_slim_cache_duration / 60)
                    }
                else:
                    return {
                        'has_cache': False,
                        'is_valid': False,
                        'cache_time': None,
                        'remaining_minutes': 0,
                        'total_duration_minutes': int(self.can_slim_cache_duration / 60)
                    }
            else:
                # 获取所有缓存的信息
                cache_count = len(self.can_slim_cache)
                valid_count = sum(1 for entry in self.can_slim_cache.values() if self._is_cache_valid(entry))
                
                return {
                    'total_cache_count': cache_count,
                    'valid_cache_count': valid_count,
                    'expired_cache_count': cache_count - valid_count,
                    'cache_duration_minutes': int(self.can_slim_cache_duration / 60)
                }
        except Exception as e:
            self.logger.error(f"获取缓存信息失败: {str(e)}")
            return {'error': str(e)}

    # 市场扫描缓存相关方法
    def _load_market_scan_cache(self):
        """加载市场扫描缓存从文件"""
        try:
            if os.path.exists(self.market_scan_cache_file):
                with open(self.market_scan_cache_file, 'r', encoding='utf-8') as f:
                    self.market_scan_cache = json.load(f)
                self.logger.info("市场扫描缓存加载成功")
            else:
                self.market_scan_cache = {}
        except Exception as e:
            self.logger.error(f"加载市场扫描缓存失败: {str(e)}")
            self.market_scan_cache = {}

    def _save_market_scan_cache(self):
        """保存市场扫描缓存到文件"""
        try:
            with open(self.market_scan_cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.market_scan_cache, f, ensure_ascii=False, indent=2)
            self.logger.info("市场扫描缓存保存成功")
        except Exception as e:
            self.logger.error(f"保存市场扫描缓存失败: {str(e)}")

    def _get_market_scan_cache_key(self, stock_code: str, market_type: str = 'A') -> str:
        """生成市场扫描缓存键"""
        return f"market_scan_{stock_code}_{market_type}"

    def _is_market_scan_cache_valid(self, cache_entry: dict) -> bool:
        """检查市场扫描缓存是否有效（未过期）"""
        try:
            cache_time = cache_entry.get('cache_time', 0)
            current_time = time.time()
            return (current_time - cache_time) < self.market_scan_cache_duration
        except Exception:
            return False

    def _get_market_scan_from_cache(self, stock_code: str, market_type: str = 'A') -> dict:
        """从缓存获取市场扫描结果"""
        try:
            # 先加载最新缓存
            self._load_market_scan_cache()
            
            cache_key = self._get_market_scan_cache_key(stock_code, market_type)
            
            if cache_key in self.market_scan_cache:
                cache_entry = self.market_scan_cache[cache_key]
                
                if self._is_market_scan_cache_valid(cache_entry):
                    self.logger.info(f"从缓存获取市场扫描结果: {stock_code}")
                    result = cache_entry.copy()
                    result['from_cache'] = True
                    result['cache_remaining'] = self.market_scan_cache_duration - (time.time() - cache_entry.get('cache_time', 0))
                    return result
                else:
                    # 缓存过期，删除
                    del self.market_scan_cache[cache_key]
                    self._save_market_scan_cache()
                    
            return None
        except Exception as e:
            self.logger.error(f"获取市场扫描缓存失败: {str(e)}")
            return None

    def _save_market_scan_to_cache(self, stock_code: str, result: dict, market_type: str = 'A'):
        """保存市场扫描结果到缓存"""
        try:
            cache_key = self._get_market_scan_cache_key(stock_code, market_type)
            
            # 添加缓存时间戳
            cached_result = result.copy()
            cached_result['cache_time'] = time.time()
            cached_result['from_cache'] = False
            
            self.market_scan_cache[cache_key] = cached_result
            self._save_market_scan_cache()
            
            self.logger.info(f"市场扫描结果已缓存: {stock_code}")
        except Exception as e:
            self.logger.error(f"保存市场扫描缓存失败: {str(e)}")

    def clear_market_scan_cache(self, stock_code: str = None, market_type: str = 'A', index_code: str = None):
        """清除市场扫描缓存"""
        try:
            self._load_market_scan_cache()
            
            if stock_code:
                # 清除特定股票的缓存
                cache_key = self._get_market_scan_cache_key(stock_code, market_type)
                if cache_key in self.market_scan_cache:
                    del self.market_scan_cache[cache_key]
                    self.logger.info(f"已清除 {stock_code} 的市场扫描缓存")
            elif index_code:
                # 清除特定指数下所有股票的缓存
                keys_to_delete = []
                for key in self.market_scan_cache.keys():
                    if f"_{market_type}" in key:
                        keys_to_delete.append(key)
                
                for key in keys_to_delete:
                    del self.market_scan_cache[key]
                
                self.logger.info(f"已清除指数 {index_code} 的所有市场扫描缓存")
            else:
                # 清除所有缓存
                self.market_scan_cache = {}
                self.logger.info("已清除所有市场扫描缓存")
                
            self._save_market_scan_cache()
            return True
        except Exception as e:
            self.logger.error(f"清除市场扫描缓存失败: {str(e)}")
            return False

    def get_market_scan_cache_info(self, stock_code: str = None, market_type: str = 'A') -> dict:
        """获取市场扫描缓存信息"""
        try:
            self._load_market_scan_cache()
            
            if stock_code:
                # 获取特定股票的缓存信息
                cache_key = self._get_market_scan_cache_key(stock_code, market_type)
                if cache_key in self.market_scan_cache:
                    cache_entry = self.market_scan_cache[cache_key]
                    cache_time = cache_entry.get('cache_time', 0)
                    remaining_time = self.market_scan_cache_duration - (time.time() - cache_time)
                    
                    return {
                        'has_cache': True,
                        'is_valid': self._is_market_scan_cache_valid(cache_entry),
                        'cache_time': datetime.fromtimestamp(cache_time).isoformat(),
                        'remaining_minutes': max(0, int(remaining_time / 60)),
                        'total_duration_minutes': int(self.market_scan_cache_duration / 60)
                    }
                else:
                    return {
                        'has_cache': False,
                        'is_valid': False,
                        'cache_time': None,
                        'remaining_minutes': 0,
                        'total_duration_minutes': int(self.market_scan_cache_duration / 60)
                    }
            else:
                # 获取所有缓存的信息
                cache_count = len(self.market_scan_cache)
                valid_count = sum(1 for entry in self.market_scan_cache.values() if self._is_market_scan_cache_valid(entry))
                
                return {
                    'total_cache_count': cache_count,
                    'valid_cache_count': valid_count,
                    'expired_cache_count': cache_count - valid_count,
                    'cache_duration_minutes': int(self.market_scan_cache_duration / 60)
                }
        except Exception as e:
            self.logger.error(f"获取市场扫描缓存信息失败: {str(e)}")
            return {'error': str(e)}