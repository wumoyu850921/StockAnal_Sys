#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
反防爬虫策略模块
"""

import time
import random
import requests
import akshare as ak


class AntiCrawlerStrategy:
    """反防爬虫策略类"""
    
    def __init__(self):
        # 预定义的User-Agent列表，避免依赖外部服务
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
        self.session = requests.Session()
        self.last_request_time = 0
        self.request_count = 0
        self.setup_session()
    
    def setup_session(self):
        """设置会话参数"""
        # 设置随机User-Agent
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        })
        
        # 禁用代理
        self.session.proxies = {}
        
        # 设置超时
        self.session.timeout = 30
    
    def get_random_delay(self, base_delay=2.0, variance=1.0):
        """获取随机延迟时间"""
        return base_delay + random.uniform(-variance, variance)
    
    def should_delay(self):
        """检查是否需要延迟"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        # 基础延迟
        min_delay = 2.0
        
        # 根据请求频率调整延迟
        if self.request_count > 20:
            min_delay = 5.0  # 高频率时增加延迟
        elif self.request_count > 10:
            min_delay = 3.0
        
        if time_since_last < min_delay:
            delay = min_delay - time_since_last + random.uniform(0.5, 1.5)
            return delay
        return 0
    
    def wait_if_needed(self):
        """如果需要则等待"""
        delay = self.should_delay()
        if delay > 0:
            time.sleep(delay)
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        # 每50次请求重置计数器和更新User-Agent
        if self.request_count % 50 == 0:
            self.reset_session()
    
    def reset_session(self):
        """重置会话"""
        time.sleep(random.uniform(10, 20))  # 长时间休息
        self.setup_session()
        self.request_count = 0
    
    def safe_akshare_call(self, func, *args, max_retries=5, **kwargs):
        """安全的AKShare调用，带有反防爬虫策略"""
        for attempt in range(max_retries):
            try:
                # 等待策略
                self.wait_if_needed()
                
                # 执行AKShare函数
                result = func(*args, **kwargs)
                
                # 成功后短暂休息
                time.sleep(random.uniform(0.5, 1.0))
                return result
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # 检查是否是防爬虫相关错误
                is_anti_crawler = any(keyword in error_msg for keyword in [
                    'max retries exceeded', 'proxy error', 'connection aborted',
                    'remote end closed', 'too many requests', '429', '403',
                    'blocked', 'forbidden', 'rate limit', 'ssl', 'eof',
                    'unexpected_eof_while_reading', 'connection reset'
                ])
                
                if is_anti_crawler:
                    # 防爬虫检测，采用更激进的等待策略
                    wait_time = min(30 * (2 ** attempt), 300)  # 最大5分钟
                    print(f"检测到防爬虫/网络限制 (尝试 {attempt + 1}/{max_retries}), 等待 {wait_time} 秒...")
                    time.sleep(wait_time)
                    
                    # 重置会话
                    self.reset_session()
                else:
                    # 其他类型错误，使用普通重试策略
                    wait_time = min(5 * (2 ** attempt), 30)
                    print(f"网络错误 (尝试 {attempt + 1}/{max_retries}), 等待 {wait_time} 秒: {e}")
                    time.sleep(wait_time)
        
        # 最后尝试直接调用一次，不使用策略
        print(f"所有策略重试失败，尝试直接调用...")
        try:
            return func(*args, **kwargs)
        except Exception as final_e:
            raise Exception(f"经过 {max_retries} 次重试和直接调用后仍然失败: {final_e}")


# 全局反防爬虫实例
anti_crawler = AntiCrawlerStrategy()


def safe_stock_zh_a_hist(symbol, **kwargs):
    """安全的股票历史数据获取"""
    return anti_crawler.safe_akshare_call(ak.stock_zh_a_hist, symbol=symbol, **kwargs)


def safe_stock_zh_a_spot_em():
    """安全的实时股票数据获取"""
    return anti_crawler.safe_akshare_call(ak.stock_zh_a_spot_em)


def safe_akshare_call(func, *args, **kwargs):
    """通用的安全AKShare调用"""
    return anti_crawler.safe_akshare_call(func, *args, **kwargs)