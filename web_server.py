# -*- coding: utf-8 -*-
"""
智能分析系统（股票） - 股票市场数据分析系统
修改：熊猫大侠
版本：v2.1.0
"""
# web_server.py

import numpy as np
import pandas as pd
from flask import Flask, render_template, request, jsonify, redirect, url_for
from stock_analyzer import StockAnalyzer
from us_stock_service import USStockService
import threading
import logging
from logging.handlers import RotatingFileHandler
import traceback
import os
import json
from datetime import date, datetime, timedelta
from flask_cors import CORS
import time
from flask_caching import Cache
import threading
import sys
from flask_swagger_ui import get_swaggerui_blueprint
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from database import get_session, StockInfo, AnalysisResult, Portfolio, USE_DATABASE
from dotenv import load_dotenv
from industry_analyzer import IndustryAnalyzer
import pymysql
import akshare as ak

# 加载环境变量
load_dotenv()

# MySQL数据库配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'daiqiang',
    'database': 'stock',
    'charset': 'utf8mb4'
}

def get_mysql_connection():
    """获取MySQL数据库连接"""
    try:
        connection = pymysql.connect(**MYSQL_CONFIG)
        return connection
    except Exception as e:
        print(f"MySQL连接失败: {e}")
        return None

def get_index_stocks_with_fallback(index_code, index_name):
    """获取指数成分股，包含多种备用方案"""
    import akshare as ak
    import pandas as pd
    
    app.logger.info(f"尝试获取{index_name}({index_code})成分股")
    
    # 方法1：尝试中证指数权重接口
    try:
        app.logger.info(f"方法1: 尝试中证指数权重接口获取{index_name}")
        stocks = ak.index_stock_cons_weight_csindex(symbol=index_code)
        if not stocks.empty:
            app.logger.info(f"成功通过中证指数权重接口获取{index_name}成分股: {len(stocks)}只")
            return stocks
    except Exception as e:
        app.logger.warning(f"中证指数权重接口失败: {str(e)}")
    
    # 方法2：尝试东方财富接口
    try:
        app.logger.info(f"方法2: 尝试东方财富接口获取{index_name}")
        if index_code == '000300':
            stocks = ak.index_stock_cons(symbol="000300")
        elif index_code == '000905':
            stocks = ak.index_stock_cons(symbol="000905") 
        elif index_code == '000852':
            stocks = ak.index_stock_cons(symbol="000852")
        elif index_code == '000001':
            stocks = ak.index_stock_cons(symbol="000001")
        else:
            stocks = ak.index_stock_cons(symbol=index_code)
            
        if not stocks.empty:
            app.logger.info(f"成功通过东方财富接口获取{index_name}成分股: {len(stocks)}只")
            return stocks
    except Exception as e:
        app.logger.warning(f"东方财富接口失败: {str(e)}")
    
    # 方法3：使用预定义的股票列表作为备用方案
    app.logger.warning(f"所有接口都失败，使用预定义的{index_name}样本股票")
    fallback_stocks = get_fallback_index_stocks(index_code, index_name)
    return fallback_stocks

def get_fallback_index_stocks(index_code, index_name):
    """获取预定义的指数样本股票作为备用方案"""
    import pandas as pd
    
    # 预定义的主要指数样本股票（这些是一些知名的蓝筹股）
    fallback_data = {
        '000300': [  # 沪深300样本
            '000001', '000002', '000858', '000876', '600000', '600036', '600519', '600887',
            '000166', '002415', '300059', '600276', '601318', '000858', '002304', '600031',
            '601166', '000725', '002142', '600009', '601328', '000002', '600104', '000001'
        ],
        '000905': [  # 中证500样本
            '002027', '002415', '002304', '000725', '002142', '000876', '002352', '000166',
            '002241', '000063', '002507', '300122', '300383', '000829', '002456', '300433'
        ],
        '000852': [  # 中证1000样本
            '002179', '300750', '300724', '002409', '300760', '002410', '300760', '002463'
        ],
        '000001': [  # 上证指数样本
            '600000', '600036', '600519', '600887', '600276', '600031', '600009', '600104'
        ]
    }
    
    stock_codes = fallback_data.get(index_code, fallback_data['000300'])  # 默认使用沪深300
    
    # 创建DataFrame格式
    df = pd.DataFrame({
        'code': stock_codes,
        'name': [f'股票{code}' for code in stock_codes]
    })
    
    app.logger.info(f"返回{index_name}预定义样本股票: {len(stock_codes)}只")
    return df

def get_stocks_by_market_cap(min_market_cap=150):
    """获取市值大于指定值的股票列表（单位：亿元）"""
    try:
        import akshare as ak
        app.logger.info(f"开始获取市值大于{min_market_cap}亿的股票")
        
        # 获取A股实时数据
        stock_data = ak.stock_zh_a_spot_em()
        
        if stock_data.empty:
            app.logger.warning("获取A股实时数据为空")
            return pd.DataFrame()
        
        app.logger.info(f"获取到 {len(stock_data)} 条A股数据")
        
        # 检查是否有总市值列
        market_cap_columns = ['总市值', '市值', 'total_market_cap']
        market_cap_col = None
        
        for col in market_cap_columns:
            if col in stock_data.columns:
                market_cap_col = col
                break
        
        if market_cap_col is None:
            app.logger.error(f"未找到市值相关列，可用列: {stock_data.columns.tolist()}")
            return pd.DataFrame()
        
        app.logger.info(f"使用市值列: {market_cap_col}")
        
        # 将市值从元转换为亿元，并过滤市值大于指定值的股票
        # 注意：akshare返回的市值单位可能是元或万元，需要根据实际情况调整
        stock_data[market_cap_col] = pd.to_numeric(stock_data[market_cap_col], errors='coerce')
        
        # 假设市值单位是元，转换为亿元  
        stock_data['market_cap_billions'] = stock_data[market_cap_col] / 100000000
        
        # 过滤市值大于指定值的股票
        filtered_stocks = stock_data[stock_data['market_cap_billions'] >= min_market_cap]
        
        app.logger.info(f"过滤后得到 {len(filtered_stocks)} 只市值大于{min_market_cap}亿的股票")
        
        # 按市值降序排序
        filtered_stocks = filtered_stocks.sort_values('market_cap_billions', ascending=False)
        
        # 返回包含代码和名称的DataFrame
        if '代码' in filtered_stocks.columns and '名称' in filtered_stocks.columns:
            result = filtered_stocks[['代码', '名称', 'market_cap_billions']].copy()
            result.columns = ['代码', '股票简称', '市值(亿)']
            return result
        else:
            app.logger.error("未找到股票代码或名称列")
            return pd.DataFrame()
            
    except Exception as e:
        app.logger.error(f"获取市值股票列表时出错: {str(e)}")
        app.logger.error(traceback.format_exc())
        return pd.DataFrame()

def init_stock_tables():
    """初始化股票数据表"""
    connection = get_mysql_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # 创建股票实时数据表
        create_realtime_table = """
        CREATE TABLE IF NOT EXISTS stock_realtime_data (
          id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
          stock_code VARCHAR(10) NOT NULL COMMENT '股票代码',
          stock_name VARCHAR(50) NOT NULL COMMENT '股票名称',
          latest_price DECIMAL(10,3) COMMENT '最新价',
          change_pct DECIMAL(8,3) COMMENT '涨跌幅(%)',
          change_amount DECIMAL(10,3) COMMENT '涨跌额',
          volume BIGINT UNSIGNED COMMENT '成交量',
          turnover BIGINT UNSIGNED COMMENT '成交额',
          amplitude DECIMAL(8,3) COMMENT '振幅(%)',
          high_price DECIMAL(10,3) COMMENT '最高价',
          low_price DECIMAL(10,3) COMMENT '最低价',
          open_price DECIMAL(10,3) COMMENT '开盘价',
          prev_close DECIMAL(10,3) COMMENT '昨收价',
          volume_ratio DECIMAL(8,3) COMMENT '量比',
          turnover_rate DECIMAL(8,3) COMMENT '换手率(%)',
          pe_ratio DECIMAL(10,3) COMMENT '市盈率',
          pb_ratio DECIMAL(8,3) COMMENT '市净率',
          total_market_cap BIGINT UNSIGNED COMMENT '总市值',
          flow_market_cap BIGINT UNSIGNED COMMENT '流通市值',
          trade_date DATE NOT NULL COMMENT '交易日期',
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY uk_stock_date (stock_code, trade_date),
          INDEX idx_stock_code (stock_code),
          INDEX idx_trade_date (trade_date),
          INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票实时数据表';
        """
        
        # 创建同步日志表
        create_sync_log_table = """
        CREATE TABLE IF NOT EXISTS stock_sync_log (
          id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
          task_id VARCHAR(50) NOT NULL UNIQUE COMMENT '任务ID',
          sync_type ENUM('realtime', 'history') NOT NULL COMMENT '同步类型',
          start_date DATE NOT NULL COMMENT '开始日期',
          end_date DATE NOT NULL COMMENT '结束日期',
          status ENUM('pending', 'running', 'completed', 'failed') DEFAULT 'pending' COMMENT '状态',
          progress INT UNSIGNED DEFAULT 0 COMMENT '进度百分比',
          total_records INT UNSIGNED DEFAULT 0 COMMENT '总记录数',
          success_records INT UNSIGNED DEFAULT 0 COMMENT '成功记录数',
          failed_records INT UNSIGNED DEFAULT 0 COMMENT '失败记录数',
          success_stock_codes TEXT COMMENT '同步成功的股票代码列表(JSON格式)',
          failed_stock_codes TEXT COMMENT '同步失败的股票代码列表(JSON格式)',
          message TEXT COMMENT '结果消息',
          error_details TEXT COMMENT '错误详情',
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          start_time TIMESTAMP NULL DEFAULT NULL COMMENT '开始时间',
          end_time TIMESTAMP NULL DEFAULT NULL COMMENT '结束时间',
          INDEX idx_task_id (task_id),
          INDEX idx_sync_type (sync_type),
          INDEX idx_status (status),
          INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据同步日志表';
        """
        
        cursor.execute(create_realtime_table)
        cursor.execute(create_sync_log_table)
        
        # 检查并添加新字段
        upgrade_stock_sync_log_table(cursor)
        
        connection.commit()
        print("股票数据表初始化成功")
        return True
        
    except Exception as e:
        print(f"初始化数据表失败: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()

def upgrade_stock_sync_log_table(cursor):
    """升级stock_sync_log表，添加成功和失败股票代码字段"""
    try:
        # 检查字段是否已存在
        cursor.execute("SHOW COLUMNS FROM stock_sync_log LIKE 'success_stock_codes'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE stock_sync_log ADD COLUMN success_stock_codes TEXT COMMENT '同步成功的股票代码列表(JSON格式)'")
            
        cursor.execute("SHOW COLUMNS FROM stock_sync_log LIKE 'failed_stock_codes'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE stock_sync_log ADD COLUMN failed_stock_codes TEXT COMMENT '同步失败的股票代码列表(JSON格式)'")
            
        print("股票同步日志表升级成功")
    except Exception as e:
        print(f"升级股票同步日志表失败: {e}")

def insert_stock_data_to_db(stock_data, trade_date):
    """将股票数据插入数据库"""
    if stock_data.empty:
        return 0, 0, []
    
    connection = get_mysql_connection()
    if not connection:
        return 0, len(stock_data), ["数据库连接失败"]
    
    success_count = 0
    failed_count = 0
    error_messages = []
    
    try:
        cursor = connection.cursor()
        
        # 准备插入SQL语句
        insert_sql = """
        INSERT INTO stock_realtime_data (
            stock_code, stock_name, latest_price, change_pct, change_amount,
            volume, turnover, amplitude, high_price, low_price, open_price,
            prev_close, volume_ratio, turnover_rate, pe_ratio, pb_ratio,
            total_market_cap, flow_market_cap, trade_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON DUPLICATE KEY UPDATE
            stock_name = VALUES(stock_name),
            latest_price = VALUES(latest_price),
            change_pct = VALUES(change_pct),
            change_amount = VALUES(change_amount),
            volume = VALUES(volume),
            turnover = VALUES(turnover),
            amplitude = VALUES(amplitude),
            high_price = VALUES(high_price),
            low_price = VALUES(low_price),
            open_price = VALUES(open_price),
            prev_close = VALUES(prev_close),
            volume_ratio = VALUES(volume_ratio),
            turnover_rate = VALUES(turnover_rate),
            pe_ratio = VALUES(pe_ratio),
            pb_ratio = VALUES(pb_ratio),
            total_market_cap = VALUES(total_market_cap),
            flow_market_cap = VALUES(flow_market_cap),
            updated_at = CURRENT_TIMESTAMP
        """
        
        # 批量插入数据
        batch_size = 100
        for i in range(0, len(stock_data), batch_size):
            batch_data = stock_data.iloc[i:i + batch_size]
            records = []
            
            for _, row in batch_data.iterrows():
                try:
                    record = (
                        str(row.get('代码', '')),
                        str(row.get('名称', '')),
                        float(row.get('最新价', 0)) if pd.notna(row.get('最新价')) else None,
                        float(row.get('涨跌幅', 0)) if pd.notna(row.get('涨跌幅')) else None,
                        float(row.get('涨跌额', 0)) if pd.notna(row.get('涨跌额')) else None,
                        int(row.get('成交量', 0)) if pd.notna(row.get('成交量')) else None,
                        int(row.get('成交额', 0)) if pd.notna(row.get('成交额')) else None,
                        float(row.get('振幅', 0)) if pd.notna(row.get('振幅')) else None,
                        float(row.get('最高', 0)) if pd.notna(row.get('最高')) else None,
                        float(row.get('最低', 0)) if pd.notna(row.get('最低')) else None,
                        float(row.get('今开', 0)) if pd.notna(row.get('今开')) else None,
                        float(row.get('昨收', 0)) if pd.notna(row.get('昨收')) else None,
                        float(row.get('量比', 0)) if pd.notna(row.get('量比')) else None,
                        float(row.get('换手率', 0)) if pd.notna(row.get('换手率')) else None,
                        float(row.get('市盈率-动态', 0)) if pd.notna(row.get('市盈率-动态')) else None,
                        float(row.get('市净率', 0)) if pd.notna(row.get('市净率')) else None,
                        int(row.get('总市值', 0)) if pd.notna(row.get('总市值')) else None,
                        int(row.get('流通市值', 0)) if pd.notna(row.get('流通市值')) else None,
                        trade_date
                    )
                    records.append(record)
                except Exception as e:
                    failed_count += 1
                    error_messages.append(f"处理股票 {row.get('代码', 'unknown')} 失败: {str(e)}")
            
            if records:
                try:
                    cursor.executemany(insert_sql, records)
                    connection.commit()
                    success_count += len(records)
                except Exception as e:
                    failed_count += len(records)
                    error_messages.append(f"批量插入失败: {str(e)}")
                    connection.rollback()
        
        return success_count, failed_count, error_messages
        
    except Exception as e:
        error_messages.append(f"数据库操作失败: {str(e)}")
        return success_count, failed_count, error_messages
    finally:
        cursor.close()
        connection.close()

def insert_sync_log_to_db(task_id, sync_type, start_date, end_date, status, progress=0, 
                         total_records=0, success_records=0, failed_records=0, 
                         message=None, error_details=None, success_stock_codes=None, failed_stock_codes=None):
    """将同步日志插入数据库"""
    import json
    
    connection = get_mysql_connection()
    if not connection:
        return False
    
    # 将股票代码列表转换为JSON字符串
    success_codes_json = json.dumps(success_stock_codes) if success_stock_codes else None
    failed_codes_json = json.dumps(failed_stock_codes) if failed_stock_codes else None
    
    try:
        cursor = connection.cursor()
        
        # 检查记录是否已存在
        check_sql = "SELECT id FROM stock_sync_log WHERE task_id = %s"
        cursor.execute(check_sql, (task_id,))
        existing = cursor.fetchone()
        
        if existing:
            # 更新现有记录
            update_sql = """
            UPDATE stock_sync_log SET
                status = %s, progress = %s, total_records = %s, 
                success_records = %s, failed_records = %s,
                success_stock_codes = %s, failed_stock_codes = %s,
                message = %s, error_details = %s,
                updated_at = CURRENT_TIMESTAMP,
                end_time = CASE WHEN %s IN ('completed', 'failed') THEN CURRENT_TIMESTAMP ELSE end_time END
            WHERE task_id = %s
            """
            cursor.execute(update_sql, (
                status, progress, total_records, success_records, failed_records,
                success_codes_json, failed_codes_json, message, error_details, status, task_id
            ))
        else:
            # 插入新记录
            insert_sql = """
            INSERT INTO stock_sync_log (
                task_id, sync_type, start_date, end_date, status, progress,
                total_records, success_records, failed_records, 
                success_stock_codes, failed_stock_codes, message, error_details,
                start_time
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
            )
            """
            cursor.execute(insert_sql, (
                task_id, sync_type, start_date, end_date, status, progress,
                total_records, success_records, failed_records,
                success_codes_json, failed_codes_json, message, error_details
            ))
        
        connection.commit()
        return True
        
    except Exception as e:
        print(f"插入同步日志失败: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()

def insert_stock_history_to_db(stock_code, hist_data):
    """将单只股票历史数据插入数据库"""
    if hist_data.empty:
        return 0, 0, []
    
    connection = get_mysql_connection()
    if not connection:
        return 0, len(hist_data), ["数据库连接失败"]
    
    success_count = 0
    failed_count = 0
    error_messages = []
    
    try:
        cursor = connection.cursor()
        
        # 准备插入SQL语句
        insert_sql = """
        INSERT INTO stock_history_data (
            stock_code, trade_date, open_price, high_price, low_price, close_price,
            volume, turnover, amplitude, change_pct, change_amount, turnover_rate
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON DUPLICATE KEY UPDATE
            open_price = VALUES(open_price),
            high_price = VALUES(high_price),
            low_price = VALUES(low_price),
            close_price = VALUES(close_price),
            volume = VALUES(volume),
            turnover = VALUES(turnover),
            amplitude = VALUES(amplitude),
            change_pct = VALUES(change_pct),
            change_amount = VALUES(change_amount),
            turnover_rate = VALUES(turnover_rate),
            updated_at = CURRENT_TIMESTAMP
        """
        
        # 批量插入数据
        batch_size = 100
        for i in range(0, len(hist_data), batch_size):
            batch_data = hist_data.iloc[i:i + batch_size]
            records = []
            
            for _, row in batch_data.iterrows():
                try:
                    # AKShare历史数据字段映射
                    record = (
                        stock_code,
                        pd.to_datetime(str(row.get('日期', ''))).strftime('%Y-%m-%d') if pd.notna(row.get('日期')) else None,
                        float(row.get('开盘', 0)) if pd.notna(row.get('开盘')) else None,
                        float(row.get('最高', 0)) if pd.notna(row.get('最高')) else None,
                        float(row.get('最低', 0)) if pd.notna(row.get('最低')) else None,
                        float(row.get('收盘', 0)) if pd.notna(row.get('收盘')) else None,
                        int(row.get('成交量', 0)) if pd.notna(row.get('成交量')) else None,
                        float(row.get('成交额', 0)) if pd.notna(row.get('成交额')) else None,
                        float(row.get('振幅', 0)) if pd.notna(row.get('振幅')) else None,
                        float(row.get('涨跌幅', 0)) if pd.notna(row.get('涨跌幅')) else None,
                        float(row.get('涨跌额', 0)) if pd.notna(row.get('涨跌额')) else None,
                        float(row.get('换手率', 0)) if pd.notna(row.get('换手率')) else None
                    )
                    
                    if record[1]:  # 确保有有效的交易日期
                        records.append(record)
                except Exception as e:
                    failed_count += 1
                    error_messages.append(f"处理股票 {stock_code} 数据失败: {str(e)}")
            
            if records:
                try:
                    cursor.executemany(insert_sql, records)
                    connection.commit()
                    success_count += len(records)
                except Exception as e:
                    failed_count += len(records)
                    error_messages.append(f"股票 {stock_code} 批量插入失败: {str(e)}")
                    connection.rollback()
        
        return success_count, failed_count, error_messages
        
    except Exception as e:
        error_messages.append(f"股票 {stock_code} 数据库操作失败: {str(e)}")
        return success_count, failed_count, error_messages
    finally:
        cursor.close()
        connection.close()

def insert_stock_info_to_db(stock_data):
    """将股票基本信息插入数据库"""
    if stock_data.empty:
        return 0, 0, []
    
    connection = get_mysql_connection()
    if not connection:
        return 0, len(stock_data), ["数据库连接失败"]
    
    success_count = 0
    failed_count = 0
    error_messages = []
    
    try:
        cursor = connection.cursor()
        
        # 准备插入SQL语句
        insert_sql = """
        INSERT INTO stock_info (
            stock_code, stock_name, market, latest_price, change_pct, change_amount,
            volume, turnover, market_cap, pe_ratio, pb_ratio, is_active
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON DUPLICATE KEY UPDATE
            stock_name = VALUES(stock_name),
            latest_price = VALUES(latest_price),
            change_pct = VALUES(change_pct),
            change_amount = VALUES(change_amount),
            volume = VALUES(volume),
            turnover = VALUES(turnover),
            market_cap = VALUES(market_cap),
            pe_ratio = VALUES(pe_ratio),
            pb_ratio = VALUES(pb_ratio),
            is_active = VALUES(is_active),
            updated_at = CURRENT_TIMESTAMP
        """
        
        # 判断市场类型的函数
        def get_market_type(stock_code):
            if stock_code.startswith('000') or stock_code.startswith('002') or stock_code.startswith('003'):
                return '深交所主板'
            elif stock_code.startswith('300'):
                return '创业板'
            elif stock_code.startswith('600') or stock_code.startswith('601') or stock_code.startswith('603') or stock_code.startswith('605'):
                return '上交所主板'
            elif stock_code.startswith('688'):
                return '科创板'
            elif stock_code.startswith('430') or stock_code.startswith('831') or stock_code.startswith('871'):
                return '新三板'
            else:
                return '其他'
        
        # 批量插入数据
        batch_size = 100
        for i in range(0, len(stock_data), batch_size):
            batch_data = stock_data.iloc[i:i + batch_size]
            records = []
            
            for _, row in batch_data.iterrows():
                try:
                    stock_code = str(row.get('代码', ''))
                    record = (
                        stock_code,
                        str(row.get('名称', '')),
                        get_market_type(stock_code),
                        float(row.get('最新价', 0)) if pd.notna(row.get('最新价')) else None,
                        float(row.get('涨跌幅', 0)) if pd.notna(row.get('涨跌幅')) else None,
                        float(row.get('涨跌额', 0)) if pd.notna(row.get('涨跌额')) else None,
                        int(row.get('成交量', 0)) if pd.notna(row.get('成交量')) else None,
                        int(row.get('成交额', 0)) if pd.notna(row.get('成交额')) else None,
                        int(row.get('总市值', 0)) if pd.notna(row.get('总市值')) else None,
                        float(row.get('市盈率-动态', 0)) if pd.notna(row.get('市盈率-动态')) else None,
                        float(row.get('市净率', 0)) if pd.notna(row.get('市净率')) else None,
                        1  # is_active 默认为活跃状态
                    )
                    records.append(record)
                except Exception as e:
                    failed_count += 1
                    error_messages.append(f"处理股票 {row.get('代码', 'unknown')} 失败: {str(e)}")
            
            if records:
                try:
                    cursor.executemany(insert_sql, records)
                    connection.commit()
                    success_count += len(records)
                except Exception as e:
                    failed_count += len(records)
                    error_messages.append(f"批量插入股票信息失败: {str(e)}")
                    connection.rollback()
        
        return success_count, failed_count, error_messages
        
    except Exception as e:
        error_messages.append(f"股票信息数据库操作失败: {str(e)}")
        return success_count, failed_count, error_messages
    finally:
        cursor.close()
        connection.close()

# 检查是否需要初始化数据库
if USE_DATABASE:
    init_db()

# 初始化股票数据表
init_stock_tables()

# 配置Swagger
SWAGGER_URL = '/api/docs'
API_URL = '/static/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "股票智能分析系统 API文档"
    }
)

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
analyzer = StockAnalyzer()
us_stock_service = USStockService()

# 配置缓存
cache_config = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}

# 如果配置了Redis，使用Redis作为缓存后端
if os.getenv('USE_REDIS_CACHE', 'False').lower() == 'true' and os.getenv('REDIS_URL'):
    cache_config = {
        'CACHE_TYPE': 'RedisCache',
        'CACHE_REDIS_URL': os.getenv('REDIS_URL'),
        'CACHE_DEFAULT_TIMEOUT': 300
    }

cache = Cache(config={'CACHE_TYPE': 'SimpleCache'})
cache.init_app(app)

app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# 确保全局变量在重新加载时不会丢失
if 'analyzer' not in globals():
    try:
        from stock_analyzer import StockAnalyzer

        analyzer = StockAnalyzer()
        print("成功初始化全局StockAnalyzer实例")
    except Exception as e:
        print(f"初始化StockAnalyzer时出错: {e}", file=sys.stderr)
        raise

# 导入新模块
from fundamental_analyzer import FundamentalAnalyzer
from capital_flow_analyzer import CapitalFlowAnalyzer
from scenario_predictor import ScenarioPredictor
from stock_qa import StockQA
from risk_monitor import RiskMonitor
from index_industry_analyzer import IndexIndustryAnalyzer
from ml_feature_engineering import StockFeatureEngineer
from ml_scoring_system import MLScoringSystem
from ml_online_learning import OnlineLearningSystem

# 初始化模块实例
fundamental_analyzer = FundamentalAnalyzer()
capital_flow_analyzer = CapitalFlowAnalyzer()
scenario_predictor = ScenarioPredictor(analyzer, os.getenv('OPENAI_API_KEY'), os.getenv('OPENAI_API_MODEL'))
stock_qa = StockQA(analyzer, os.getenv('OPENAI_API_KEY'), os.getenv('OPENAI_API_MODEL'))
risk_monitor = RiskMonitor(analyzer)
index_industry_analyzer = IndexIndustryAnalyzer(analyzer)
industry_analyzer = IndustryAnalyzer()

# 初始化ML系统实例
ml_feature_engineer = StockFeatureEngineer()
ml_scoring_system = MLScoringSystem()
ml_online_learning = OnlineLearningSystem(ml_scoring_system)

# Thread-local storage
thread_local = threading.local()

# 多线程同步相关的锁和变量
sync_results_lock = Lock()
sync_progress_lock = Lock()

def sync_single_stock_history(stock_code, start_date, end_date):
    """同步单只股票的历史数据（线程安全）"""
    try:
        import akshare as ak
        import time
        
        # 添加随机延迟避免并发请求过多
        time.sleep(0.1)
        
        # 获取单只股票历史数据
        hist_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", 
                                     start_date=start_date.replace('-', ''), 
                                     end_date=end_date.replace('-', ''))
        
        if not hist_data.empty:
            # 实现历史数据的入库逻辑
            hist_success_count, hist_failed_count, hist_errors = insert_stock_history_to_db(stock_code, hist_data)
            return {
                'stock_code': stock_code,
                'success': True,
                'success_count': hist_success_count,
                'failed_count': hist_failed_count,
                'errors': hist_errors
            }
        else:
            return {
                'stock_code': stock_code,
                'success': False,
                'success_count': 0,
                'failed_count': 0,
                'errors': [f"股票 {stock_code} 未获取到历史数据"]
            }
            
    except Exception as e:
        error_msg = str(e)
        return {
            'stock_code': stock_code,
            'success': False,
            'success_count': 0,
            'failed_count': 1,
            'errors': [f"股票 {stock_code} 同步失败: {error_msg}"]
        }

def sync_stock_history_concurrent(task_id, sync_type, start_date, end_date, stock_codes, 
                                info_success_count, info_failed_count, max_workers=8):
    """多线程并发同步股票历史数据"""
    total_stocks = len(stock_codes)
    success_records = 0
    failed_records = 0
    error_messages = []
    success_stock_codes = []
    failed_stock_codes = []
    
    app.logger.info(f"启动多线程并发同步，线程数: {max_workers}, 股票数: {total_stocks}")
    
    # 使用线程池执行器
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_stock = {
            executor.submit(sync_single_stock_history, stock_code, start_date, end_date): stock_code 
            for stock_code in stock_codes
        }
        
        completed = 0
        
        # 处理完成的任务
        for future in as_completed(future_to_stock):
            stock_code = future_to_stock[future]
            
            try:
                result = future.result()
                
                # 线程安全地更新结果
                with sync_results_lock:
                    success_records += result['success_count']
                    failed_records += result['failed_count']
                    error_messages.extend(result['errors'])
                    
                    if result['success']:
                        success_stock_codes.append(result['stock_code'])
                        app.logger.info(f"股票 {result['stock_code']} 历史数据: 成功入库 {result['success_count']} 条记录")
                    else:
                        failed_stock_codes.append(result['stock_code'])
                        app.logger.warning(f"股票 {result['stock_code']} 同步失败")
                
                completed += 1
                
                # 线程安全地更新进度
                with sync_progress_lock:
                    progress = 30 + int((completed / total_stocks) * 60)
                    update_sync_task_status(task_id, TASK_RUNNING, progress=progress)
                    
                    # 每完成20只股票更新一次数据库
                    if completed % 20 == 0 or completed == total_stocks:
                        insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_RUNNING, 
                                            progress=progress, total_records=total_stocks,
                                            success_records=info_success_count + success_records, 
                                            failed_records=info_failed_count + failed_records,
                                            success_stock_codes=success_stock_codes, 
                                            failed_stock_codes=failed_stock_codes,
                                            message=f'多线程同步中，已完成 {completed}/{total_stocks} 只股票')
                        app.logger.info(f"批量更新: 已完成 {completed}/{total_stocks} 只股票，成功 {len(success_stock_codes)} 只")
                
            except Exception as e:
                app.logger.error(f"处理股票 {stock_code} 结果时出错: {str(e)}")
                with sync_results_lock:
                    failed_records += 1
                    failed_stock_codes.append(stock_code)
                    error_messages.append(f"股票 {stock_code} 处理结果失败: {str(e)}")
    
    app.logger.info(f"多线程并发同步完成: 总计 {total_stocks} 只股票, 成功 {len(success_stock_codes)} 只, 失败 {len(failed_stock_codes)} 只")
    
    return success_records, failed_records, error_messages, success_stock_codes, failed_stock_codes

def sync_stock_history_concurrent_retry(task_id, sync_type, start_date, end_date, failed_stock_codes, 
                                      original_task_id, max_workers=4):
    """多线程并发重新同步失败的股票历史数据"""
    total_stocks = len(failed_stock_codes)
    success_records = 0
    failed_records = 0
    error_messages = []
    success_stock_codes = []
    new_failed_stock_codes = []
    
    app.logger.info(f"启动多线程并发重试同步，线程数: {max_workers}, 失败股票数: {total_stocks}")
    
    # 使用线程池执行器（使用较少的线程数避免对API压力过大）
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_stock = {
            executor.submit(sync_single_stock_history, stock_code, start_date, end_date): stock_code 
            for stock_code in failed_stock_codes
        }
        
        completed = 0
        
        # 处理完成的任务
        for future in as_completed(future_to_stock):
            stock_code = future_to_stock[future]
            
            try:
                result = future.result()
                
                # 线程安全地更新结果
                with sync_results_lock:
                    success_records += result['success_count']
                    failed_records += result['failed_count']
                    error_messages.extend(result['errors'])
                    
                    if result['success']:
                        success_stock_codes.append(result['stock_code'])
                        app.logger.info(f"重试成功：股票 {result['stock_code']} 历史数据: 成功入库 {result['success_count']} 条记录")
                    else:
                        new_failed_stock_codes.append(result['stock_code'])
                        app.logger.warning(f"重试失败：股票 {result['stock_code']} 仍然无法同步 原因: {result['errors']}")
                
                completed += 1
                
                # 线程安全地更新进度
                with sync_progress_lock:
                    progress = 10 + int((completed / total_stocks) * 80)
                    update_sync_task_status(task_id, TASK_RUNNING, progress=progress)
                    
                    # 每完成10只股票更新一次数据库并且更新原始任务
                    if completed % 10 == 0 or completed == total_stocks:
                        # 更新原始任务的股票代码列表
                        update_original_task_stock_codes(original_task_id, success_stock_codes[-min(10, len(success_stock_codes)):], 
                                                       new_failed_stock_codes[-min(10, len(new_failed_stock_codes)):])
                        
                        # 只在未完成时更新为running状态，避免覆盖最终的completed状态
                        if completed < total_stocks:
                            insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_RUNNING, 
                                                progress=progress, total_records=total_stocks,
                                                success_records=success_records, 
                                                failed_records=failed_records,
                                                success_stock_codes=success_stock_codes, 
                                                failed_stock_codes=new_failed_stock_codes,
                                                message=f'重试同步中，已完成 {completed}/{total_stocks} 只股票')
                        app.logger.info(f"批量更新: 重试已完成 {completed}/{total_stocks} 只股票，成功 {len(success_stock_codes)} 只")
                
            except Exception as e:
                app.logger.error(f"处理重试股票 {stock_code} 结果时出错: {str(e)}")
                with sync_results_lock:
                    failed_records += 1
                    new_failed_stock_codes.append(stock_code)
                    error_messages.append(f"股票 {stock_code} 重试处理结果失败: {str(e)}")
    
    app.logger.info(f"多线程并发重试同步完成: 总计 {total_stocks} 只失败股票, 重试成功 {len(success_stock_codes)} 只, 仍然失败 {len(new_failed_stock_codes)} 只")
    
    return success_records, failed_records, error_messages, success_stock_codes, new_failed_stock_codes


def get_analyzer():
    """获取线程本地的分析器实例"""
    # 如果线程本地存储中没有分析器实例，创建一个新的
    if not hasattr(thread_local, 'analyzer'):
        thread_local.analyzer = StockAnalyzer()
    return thread_local.analyzer


# 配置日志
logging.basicConfig(level=logging.INFO)
handler = RotatingFileHandler('flask_app.log', maxBytes=10000000, backupCount=5)
handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
))
app.logger.addHandler(handler)

# 扩展任务管理系统以支持不同类型的任务
task_types = {
    'scan': 'market_scan',  # 市场扫描任务
    'analysis': 'stock_analysis'  # 个股分析任务
}

# 任务数据存储
tasks = {
    'market_scan': {},  # 原来的scan_tasks
    'stock_analysis': {}  # 新的个股分析任务
}


def get_task_store(task_type):
    """获取指定类型的任务存储"""
    return tasks.get(task_type, {})


def generate_task_key(task_type, **params):
    """生成任务键"""
    if task_type == 'stock_analysis':
        # 对于个股分析，使用股票代码和市场类型作为键
        return f"{params.get('stock_code')}_{params.get('market_type', 'A')}"
    return None  # 其他任务类型不使用预生成的键


def get_or_create_task(task_type, **params):
    """获取或创建任务"""
    store = get_task_store(task_type)
    task_key = generate_task_key(task_type, **params)

    # 检查是否有现有任务
    if task_key and task_key in store:
        task = store[task_key]
        # 检查任务是否仍然有效
        if task['status'] in [TASK_PENDING, TASK_RUNNING]:
            return task['id'], task, False
        if task['status'] == TASK_COMPLETED and 'result' in task:
            # 任务已完成且有结果，重用它
            return task['id'], task, False

    # 创建新任务
    task_id = generate_task_id()
    task = {
        'id': task_id,
        'key': task_key,  # 存储任务键以便以后查找
        'type': task_type,
        'status': TASK_PENDING,
        'progress': 0,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'params': params
    }

    with task_lock:
        if task_key:
            store[task_key] = task
        store[task_id] = task

    return task_id, task, True


# 添加到web_server.py顶部
# 任务管理系统
scan_tasks = {}  # 存储扫描任务的状态和结果
task_lock = threading.Lock()  # 用于线程安全操作

# 任务状态常量
TASK_PENDING = 'pending'
TASK_RUNNING = 'running'
TASK_COMPLETED = 'completed'
TASK_FAILED = 'failed'


def generate_task_id():
    """生成唯一的任务ID"""
    import uuid
    return str(uuid.uuid4())


def start_market_scan_task_status(task_id, status, progress=None, result=None, error=None):
    """更新任务状态 - 保持原有签名"""
    with task_lock:
        if task_id in scan_tasks:
            task = scan_tasks[task_id]
            task['status'] = status
            if progress is not None:
                task['progress'] = progress
            if result is not None:
                task['result'] = result
            if error is not None:
                task['error'] = error
            task['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def update_task_status(task_type, task_id, status, progress=None, result=None, error=None):
    """更新任务状态"""
    store = get_task_store(task_type)
    with task_lock:
        if task_id in store:
            task = store[task_id]
            task['status'] = status
            if progress is not None:
                task['progress'] = progress
            if result is not None:
                task['result'] = result
            if error is not None:
                task['error'] = error
            task['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 更新键索引的任务
            if 'key' in task and task['key'] in store:
                store[task['key']] = task


analysis_tasks = {}


def get_or_create_analysis_task(stock_code, market_type='A'):
    """获取或创建个股分析任务"""
    # 创建一个键，用于查找现有任务
    task_key = f"{stock_code}_{market_type}"

    with task_lock:
        # 检查是否有现有任务
        for task_id, task in analysis_tasks.items():
            if task.get('key') == task_key:
                # 检查任务是否仍然有效
                if task['status'] in [TASK_PENDING, TASK_RUNNING]:
                    return task_id, task, False
                if task['status'] == TASK_COMPLETED and 'result' in task:
                    # 任务已完成且有结果，重用它
                    return task_id, task, False

        # 创建新任务
        task_id = generate_task_id()
        task = {
            'id': task_id,
            'key': task_key,
            'status': TASK_PENDING,
            'progress': 0,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params': {
                'stock_code': stock_code,
                'market_type': market_type
            }
        }

        analysis_tasks[task_id] = task

        return task_id, task, True


def update_analysis_task(task_id, status, progress=None, result=None, error=None):
    """更新个股分析任务状态"""
    with task_lock:
        if task_id in analysis_tasks:
            task = analysis_tasks[task_id]
            task['status'] = status
            if progress is not None:
                task['progress'] = progress
            if result is not None:
                task['result'] = result
            if error is not None:
                task['error'] = error
            task['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# Define the custom JSON encoder


# In web_server.py, update the convert_numpy_types function to handle NaN values

# Function to convert NumPy types to Python native types
def convert_numpy_types(obj):
    """Recursively converts NumPy types in dictionaries and lists to Python native types"""
    try:
        import numpy as np
        import math

        if isinstance(obj, dict):
            return {key: convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [convert_numpy_types(item) for item in obj]
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            # Handle NaN and Infinity specifically
            if np.isnan(obj):
                return None
            elif np.isinf(obj):
                return None if obj < 0 else 1e308  # Use a very large number for +Infinity
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        # Handle Python's own float NaN and Infinity
        elif isinstance(obj, float):
            if math.isnan(obj):
                return None
            elif math.isinf(obj):
                return None
            return obj
        # 添加对date和datetime类型的处理
        elif isinstance(obj, (date, datetime)):
            return obj.isoformat()
        else:
            return obj
    except ImportError:
        # 如果没有安装numpy，但需要处理date和datetime
        import math
        if isinstance(obj, dict):
            return {key: convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [convert_numpy_types(item) for item in obj]
        elif isinstance(obj, (date, datetime)):
            return obj.isoformat()
        # Handle Python's own float NaN and Infinity
        elif isinstance(obj, float):
            if math.isnan(obj):
                return None
            elif math.isinf(obj):
                return None
            return obj
        return obj


# Also update the NumpyJSONEncoder class
class NumpyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        # For NumPy data types
        try:
            import numpy as np
            import math
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                # Handle NaN and Infinity specifically
                if np.isnan(obj):
                    return None
                elif np.isinf(obj):
                    return None
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, np.bool_):
                return bool(obj)
            # Handle Python's own float NaN and Infinity
            elif isinstance(obj, float):
                if math.isnan(obj):
                    return None
                elif math.isinf(obj):
                    return None
                return obj
        except ImportError:
            # Handle Python's own float NaN and Infinity if numpy is not available
            import math
            if isinstance(obj, float):
                if math.isnan(obj):
                    return None
                elif math.isinf(obj):
                    return None

        # 添加对date和datetime类型的处理
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()

        return super(NumpyJSONEncoder, self).default(obj)


# Custom jsonify function that uses our encoder
def custom_jsonify(data):
    return app.response_class(
        json.dumps(convert_numpy_types(data), cls=NumpyJSONEncoder),
        mimetype='application/json'
    )


# 保持API兼容的路由
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/analyze', methods=['POST'])
def analyze():
    try:
        data = request.json
        stock_codes = data.get('stock_codes', [])
        market_type = data.get('market_type', 'A')

        if not stock_codes:
            return jsonify({'error': '请输入代码'}), 400

        app.logger.info(f"分析股票请求: {stock_codes}, 市场类型: {market_type}")

        # 设置最大处理时间，每只股票10秒
        max_time_per_stock = 10  # 秒
        max_total_time = max(30, min(60, len(stock_codes) * max_time_per_stock))  # 至少30秒，最多60秒

        start_time = time.time()
        results = []

        for stock_code in stock_codes:
            try:
                # 检查是否已超时
                if time.time() - start_time > max_total_time:
                    app.logger.warning(f"分析股票请求已超过{max_total_time}秒，提前返回已处理的{len(results)}只股票")
                    break

                # 使用线程本地缓存的分析器实例
                current_analyzer = get_analyzer()
                result = current_analyzer.quick_analyze_stock(stock_code.strip(), market_type)

                app.logger.info(
                    f"分析结果: 股票={stock_code}, 名称={result.get('stock_name', '未知')}, 行业={result.get('industry', '未知')}")
                results.append(result)
            except Exception as e:
                app.logger.error(f"分析股票 {stock_code} 时出错: {str(e)}")
                results.append({
                    'stock_code': stock_code,
                    'error': str(e),
                    'stock_name': '分析失败',
                    'industry': '未知'
                })

        return jsonify({'results': results})
    except Exception as e:
        app.logger.error(f"分析股票时出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/calculate_scores', methods=['POST'])
def api_calculate_scores():
    """
    为前端综合评分图表提供评分计算API
    接收股票代码，返回每日评分结果
    """
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')
        
        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400
        
        # 获取股票历史数据（默认1年）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        
        # 获取股票历史数据
        app.logger.info(
            f"获取股票 {stock_code} 的历史数据，市场: {market_type}, 起始日期: {start_date}, 结束日期: {end_date}")
        df = analyzer.get_stock_data(stock_code, market_type, start_date, end_date)

        # 计算技术指标
        app.logger.info(f"计算股票 {stock_code} 的技术指标")
        df = analyzer.calculate_indicators(df)
        
        # 检查数据是否为空
        if df.empty:
            app.logger.warning(f"股票 {stock_code} 的数据为空")
            return jsonify({'error': '未找到股票数据'}), 404
        
        # 为每一天计算基于过去365天数据的综合评分
        scores = []
        for i in range(len(df)):
            # 获取当前日期往前365天的数据（或者从开始到当前的所有数据）
            start_index = max(0, i - 364) # 365天包含当前天
            historical_data = df.iloc[start_index:i+1]
            
            if len(historical_data) >= 30: # 至少需要30天的数据才能计算评分
                try:
                    score = analyzer.calculate_score(historical_data, market_type)
                    scores.append({
                        'date': df.iloc[i]['date'].strftime('%Y-%m-%d'),
                        'score': score
                    })
                except Exception as e:
                    app.logger.warning(f"计算第{i}日评分失败: {str(e)}")
                    scores.append({
                        'date': df.iloc[i]['date'].strftime('%Y-%m-%d'),
                        'score': None
                    })
            else:
                scores.append({
                    'date': df.iloc[i]['date'].strftime('%Y-%m-%d'),
                    'score': None # 数据不足时为null
                })
        
        return custom_jsonify({
            'success': True,
            'data': scores,
            'total_count': len(scores),
            'valid_count': len([s for s in scores if s['score'] is not None])
        })
        
    except Exception as e:
        app.logger.error(f"计算评分时出错: {traceback.format_exc()}")
        return jsonify({'error': f'评分计算失败: {str(e)}'}), 500


@app.route('/api/north_flow_history', methods=['POST'])
def api_north_flow_history():
    try:
        data = request.json
        stock_code = data.get('stock_code')
        days = data.get('days', 10)  # 默认为10天，对应前端的默认选项

        # 计算 end_date 为当前时间
        end_date = datetime.now().strftime('%Y%m%d')

        # 计算 start_date 为 end_date 减去指定的天数
        start_date = (datetime.now() - timedelta(days=int(days))).strftime('%Y%m%d')

        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400

        # 调用北向资金历史数据方法
        from capital_flow_analyzer import CapitalFlowAnalyzer

        analyzer = CapitalFlowAnalyzer()
        result = analyzer.get_north_flow_history(stock_code, start_date, end_date)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"获取北向资金历史数据出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/search_us_stocks', methods=['GET'])
def search_us_stocks():
    try:
        keyword = request.args.get('keyword', '')
        if not keyword:
            return jsonify({'error': '请输入搜索关键词'}), 400

        results = us_stock_service.search_us_stocks(keyword)
        return jsonify({'results': results})

    except Exception as e:
        app.logger.error(f"搜索美股代码时出错: {str(e)}")
        return jsonify({'error': str(e)}), 500


# 新增可视化分析页面路由
@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')


@app.route('/stock_detail/<string:stock_code>')
def stock_detail(stock_code):
    market_type = request.args.get('market_type', 'A')
    return render_template('stock_detail.html', stock_code=stock_code, market_type=market_type)


@app.route('/portfolio')
def portfolio():
    return render_template('portfolio.html')


@app.route('/market_scan')
def market_scan():
    return render_template('market_scan.html')


# 基本面分析页面
@app.route('/fundamental')
def fundamental():
    return render_template('fundamental.html')


# 资金流向页面
@app.route('/capital_flow')
def capital_flow():
    return render_template('capital_flow.html')


# 情景预测页面
@app.route('/scenario_predict')
def scenario_predict():
    return render_template('scenario_predict.html')


# 风险监控页面
@app.route('/risk_monitor')
def risk_monitor_page():
    return render_template('risk_monitor.html')


# 智能问答页面
@app.route('/qa')
def qa_page():
    return render_template('qa.html')


# 行业分析页面
@app.route('/industry_analysis')
def industry_analysis():
    return render_template('industry_analysis.html')

# K线图页面
@app.route('/kline')
def kline_page():
    return render_template('kline.html')


def make_cache_key_with_stock():
    """创建包含股票代码的自定义缓存键"""
    path = request.path

    # 从请求体中获取股票代码
    stock_code = None
    if request.is_json:
        stock_code = request.json.get('stock_code')

    # 构建包含股票代码的键
    if stock_code:
        return f"{path}_{stock_code}"
    else:
        return path


@app.route('/api/start_stock_analysis', methods=['POST'])
def start_stock_analysis():
    """启动个股分析任务"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')

        if not stock_code:
            return jsonify({'error': '请输入股票代码'}), 400

        app.logger.info(f"准备分析股票: {stock_code}")

        # 获取或创建任务
        task_id, task, is_new = get_or_create_task(
            'stock_analysis',
            stock_code=stock_code,
            market_type=market_type
        )

        # 如果是已完成的任务，直接返回结果
        if task['status'] == TASK_COMPLETED and 'result' in task:
            app.logger.info(f"使用缓存的分析结果: {stock_code}")
            return jsonify({
                'task_id': task_id,
                'status': task['status'],
                'result': task['result']
            })

        # 如果是新创建的任务，启动后台处理
        if is_new:
            app.logger.info(f"创建新的分析任务: {task_id}")

            # 启动后台线程执行分析
            def run_analysis():
                try:
                    update_task_status('stock_analysis', task_id, TASK_RUNNING, progress=10)

                    # 执行分析
                    result = analyzer.perform_enhanced_analysis(stock_code, market_type)

                    # 更新任务状态为完成
                    update_task_status('stock_analysis', task_id, TASK_COMPLETED, progress=100, result=result)
                    app.logger.info(f"分析任务 {task_id} 完成")

                except Exception as e:
                    app.logger.error(f"分析任务 {task_id} 失败: {str(e)}")
                    app.logger.error(traceback.format_exc())
                    update_task_status('stock_analysis', task_id, TASK_FAILED, error=str(e))

            # 启动后台线程
            thread = threading.Thread(target=run_analysis)
            thread.daemon = True
            thread.start()

        # 返回任务ID和状态
        return jsonify({
            'task_id': task_id,
            'status': task['status'],
            'message': f'已启动分析任务: {stock_code}'
        })

    except Exception as e:
        app.logger.error(f"启动个股分析任务时出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/analysis_status/<task_id>', methods=['GET'])
def get_analysis_status(task_id):
    """获取个股分析任务状态"""
    store = get_task_store('stock_analysis')
    with task_lock:
        if task_id not in store:
            return jsonify({'error': '找不到指定的分析任务'}), 404

        task = store[task_id]

        # 基本状态信息
        status = {
            'id': task['id'],
            'status': task['status'],
            'progress': task.get('progress', 0),
            'created_at': task['created_at'],
            'updated_at': task['updated_at']
        }

        # 如果任务完成，包含结果
        if task['status'] == TASK_COMPLETED and 'result' in task:
            status['result'] = task['result']

        # 如果任务失败，包含错误信息
        if task['status'] == TASK_FAILED and 'error' in task:
            status['error'] = task['error']

        return custom_jsonify(status)


@app.route('/api/cancel_analysis/<task_id>', methods=['POST'])
def cancel_analysis(task_id):
    """取消个股分析任务"""
    store = get_task_store('stock_analysis')
    with task_lock:
        if task_id not in store:
            return jsonify({'error': '找不到指定的分析任务'}), 404

        task = store[task_id]

        if task['status'] in [TASK_COMPLETED, TASK_FAILED]:
            return jsonify({'message': '任务已完成或失败，无法取消'})

        # 更新状态为失败
        task['status'] = TASK_FAILED
        task['error'] = '用户取消任务'
        task['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 更新键索引的任务
        if 'key' in task and task['key'] in store:
            store[task['key']] = task

        return jsonify({'message': '任务已取消'})


# 保留原有API用于向后兼容
@app.route('/api/enhanced_analysis', methods=['POST'])
def enhanced_analysis():
    """原增强分析API的向后兼容版本"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')

        if not stock_code:
            return custom_jsonify({'error': '请输入股票代码'}), 400

        # 调用新的任务系统，但模拟同步行为
        # 这会导致和之前一样的超时问题，但保持兼容
        timeout = 300
        start_time = time.time()

        # 获取或创建任务
        task_id, task, is_new = get_or_create_task(
            'stock_analysis',
            stock_code=stock_code,
            market_type=market_type
        )

        # 如果是已完成的任务，直接返回结果
        if task['status'] == TASK_COMPLETED and 'result' in task:
            app.logger.info(f"使用缓存的分析结果: {stock_code}")
            return custom_jsonify({'result': task['result']})

        # 启动分析（如果是新任务）
        if is_new:
            # 同步执行分析
            try:
                result = analyzer.perform_enhanced_analysis(stock_code, market_type)
                update_task_status('stock_analysis', task_id, TASK_COMPLETED, progress=100, result=result)
                app.logger.info(f"分析完成: {stock_code}，耗时 {time.time() - start_time:.2f} 秒")
                return custom_jsonify({'result': result})
            except Exception as e:
                app.logger.error(f"分析过程中出错: {str(e)}")
                update_task_status('stock_analysis', task_id, TASK_FAILED, error=str(e))
                return custom_jsonify({'error': f'分析过程中出错: {str(e)}'}), 500
        else:
            # 已存在正在处理的任务，等待其完成
            max_wait = timeout - (time.time() - start_time)
            wait_interval = 0.5
            waited = 0

            while waited < max_wait:
                with task_lock:
                    current_task = store[task_id]
                    if current_task['status'] == TASK_COMPLETED and 'result' in current_task:
                        return custom_jsonify({'result': current_task['result']})
                    if current_task['status'] == TASK_FAILED:
                        error = current_task.get('error', '任务失败，无详细信息')
                        return custom_jsonify({'error': error}), 500

                time.sleep(wait_interval)
                waited += wait_interval

            # 超时
            return custom_jsonify({'error': '处理超时，请稍后重试'}), 504

    except Exception as e:
        app.logger.error(f"执行增强版分析时出错: {traceback.format_exc()}")
        return custom_jsonify({'error': str(e)}), 500


# 添加在web_server.py主代码中
@app.errorhandler(404)
def not_found(error):
    """处理404错误"""
    if request.path.startswith('/api/'):
        # 为API请求返回JSON格式的错误
        return jsonify({
            'error': '找不到请求的API端点',
            'path': request.path,
            'method': request.method
        }), 404
    # 为网页请求返回HTML错误页
    return render_template('error.html', error_code=404, message="找不到请求的页面"), 404


@app.errorhandler(500)
def server_error(error):
    """处理500错误"""
    app.logger.error(f"服务器错误: {str(error)}")
    if request.path.startswith('/api/'):
        # 为API请求返回JSON格式的错误
        return jsonify({
            'error': '服务器内部错误',
            'message': str(error)
        }), 500
    # 为网页请求返回HTML错误页
    return render_template('error.html', error_code=500, message="服务器内部错误"), 500


# Update the get_stock_data function in web_server.py to handle date formatting properly
@app.route('/api/stock_data', methods=['GET'])
def get_stock_data():
    try:
        stock_code = request.args.get('stock_code')
        market_type = request.args.get('market_type', 'A')
        period = request.args.get('period', '1y')  # 默认1年
        force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'

        if not stock_code:
            return custom_jsonify({'error': '请提供股票代码'}), 400
        
        # 缓存处理
        cache_key = f"stock_data_{stock_code}_{market_type}_{period}"
        if force_refresh:
            # 强制刷新时，先清除现有缓存
            cache.delete(cache_key)
            # 也清除分析器的数据缓存
            analyzer.data_cache.clear()
            
            # 清除相关的分析任务缓存
            analysis_store = get_task_store('stock_analysis')
            with task_lock:
                # 清除该股票的已完成分析任务，强制重新分析
                to_remove = []
                for task_id, task in analysis_store.items():
                    if (task.get('params', {}).get('stock_code') == stock_code and 
                        task.get('params', {}).get('market_type') == market_type and
                        task.get('status') == TASK_COMPLETED):
                        to_remove.append(task_id)
                
                for task_id in to_remove:
                    del analysis_store[task_id]
                    app.logger.info(f"强制刷新：已清除分析任务缓存 {task_id}")
            
            app.logger.info(f"强制刷新：已清除Flask缓存 {cache_key}、分析器数据缓存和 {len(to_remove)} 个分析任务缓存")
        else:
            # 正常情况下检查缓存
            cached_data = cache.get(cache_key)
            if cached_data:
                return cached_data

        # 根据period计算start_date
        end_date = datetime.now().strftime('%Y%m%d')
        if period == '1m':
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        elif period == '3m':
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
        elif period == '6m':
            start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')
        elif period == '1y':
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        elif period == '3y':
            start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y%m%d')  # 3年
        else:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

        # 获取股票历史数据
        app.logger.info(
            f"获取股票 {stock_code} 的历史数据，市场: {market_type}, 起始日期: {start_date}, 结束日期: {end_date}")
        df = analyzer.get_stock_data(stock_code, market_type, start_date, end_date)

        # 计算技术指标
        app.logger.info(f"计算股票 {stock_code} 的技术指标")
        df = analyzer.calculate_indicators(df)

        # 检查数据是否为空
        if df.empty:
            app.logger.warning(f"股票 {stock_code} 的数据为空")
            return custom_jsonify({'error': '未找到股票数据'}), 404

        # 将DataFrame转为JSON格式
        app.logger.info(f"将数据转换为JSON格式，行数: {len(df)}")

        # 确保日期列是字符串格式 - 修复缓存问题
        if 'date' in df.columns:
            try:
                if pd.api.types.is_datetime64_any_dtype(df['date']):
                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                else:
                    df = df.copy()
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            except Exception as e:
                app.logger.error(f"处理日期列时出错: {str(e)}")
                df['date'] = df['date'].astype(str)

        # 过滤非交易日（周末和节假日）
        if 'date' in df.columns and not df.empty:
            try:
                from utils.trading_day_filter import trading_day_filter
                original_count = len(df)
                df = trading_day_filter.filter_dataframe(df, 'date')
                app.logger.info(f"过滤非交易日后，从 {original_count} 条数据减少到 {len(df)} 条有效交易数据")
                
                # 重新格式化日期为字符串
                if pd.api.types.is_datetime64_any_dtype(df['date']):
                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                    
            except Exception as e:
                app.logger.warning(f"过滤交易日时出错，使用原始数据: {str(e)}")

        # 将NaN值替换为None
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

        records = df.to_dict('records')
        response_data = custom_jsonify({'data': records})

        # 在正常情况下设置缓存（强制刷新时不设置缓存，避免立即缓存新数据）
        if not force_refresh:
            cache.set(cache_key, response_data, timeout=300)  # 缓存5分钟
            app.logger.info(f"数据已缓存: {cache_key}")

        app.logger.info(f"数据处理完成，返回 {len(records)} 条记录")
        return response_data
    except Exception as e:
        app.logger.error(f"获取股票数据时出错: {str(e)}")
        app.logger.error(traceback.format_exc())
        return custom_jsonify({'error': str(e)}), 500


# @app.route('/api/market_scan', methods=['POST'])
# def api_market_scan():
#     try:
#         data = request.json
#         stock_list = data.get('stock_list', [])
#         min_score = data.get('min_score', 60)
#         market_type = data.get('market_type', 'A')

#         if not stock_list:
#             return jsonify({'error': '请提供股票列表'}), 400

#         # 限制股票数量，避免过长处理时间
#         if len(stock_list) > 100:
#             app.logger.warning(f"股票列表过长 ({len(stock_list)}只)，截取前100只")
#             stock_list = stock_list[:100]

#         # 执行市场扫描
#         app.logger.info(f"开始扫描 {len(stock_list)} 只股票，最低分数: {min_score}")

#         # 使用线程池优化处理
#         results = []
#         max_workers = min(10, len(stock_list))  # 最多10个工作线程

#         # 设置较长的超时时间
#         timeout = 300  # 5分钟

#         def scan_thread():
#             try:
#                 return analyzer.scan_market(stock_list, min_score, market_type)
#             except Exception as e:
#                 app.logger.error(f"扫描线程出错: {str(e)}")
#                 return []

#         thread = threading.Thread(target=lambda: results.append(scan_thread()))
#         thread.start()
#         thread.join(timeout)

#         if thread.is_alive():
#             app.logger.error(f"市场扫描超时，已扫描 {len(stock_list)} 只股票超过 {timeout} 秒")
#             return custom_jsonify({'error': '扫描超时，请减少股票数量或稍后再试'}), 504

#         if not results or not results[0]:
#             app.logger.warning("扫描结果为空")
#             return custom_jsonify({'results': []})

#         scan_results = results[0]
#         app.logger.info(f"扫描完成，找到 {len(scan_results)} 只符合条件的股票")

#         # 使用自定义JSON格式处理NumPy数据类型
#         return custom_jsonify({'results': scan_results})
#     except Exception as e:
#         app.logger.error(f"执行市场扫描时出错: {traceback.format_exc()}")
#         return custom_jsonify({'error': str(e)}), 500

@app.route('/api/start_market_scan', methods=['POST'])
def start_market_scan():
    """启动市场扫描任务"""
    try:
        data = request.json
        stock_list = data.get('stock_list', [])
        min_score = data.get('min_score', 60)
        market_type = data.get('market_type', 'A')
        start_date = data.get('start_date')  # 新增：开始日期
        end_date = data.get('end_date')      # 新增：结束日期
        force_refresh = data.get('force_refresh', False)  # 新增：强制刷新

        if not stock_list:
            return jsonify({'error': '请提供股票列表'}), 400

        # 限制股票数量，避免过长处理时间
        # if len(stock_list) > 100:
        #     app.logger.warning(f"股票列表过长 ({len(stock_list)}只)，截取前100只")
        #     stock_list = stock_list[:100]

        # 创建新任务
        task_id = generate_task_id()
        task = {
            'id': task_id,
            'status': TASK_PENDING,
            'progress': 0,
            'total': len(stock_list),
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params': {
                'stock_list': stock_list,
                'min_score': min_score,
                'market_type': market_type,
                'start_date': start_date,
                'end_date': end_date,
                'force_refresh': force_refresh
            }
        }

        with task_lock:
            scan_tasks[task_id] = task

        # 启动后台线程执行扫描
        def run_scan():
            try:
                start_market_scan_task_status(task_id, TASK_RUNNING)

                # 执行分批处理
                results = []
                total = len(stock_list)
                batch_size = 10

                for i in range(0, total, batch_size):
                    if task_id not in scan_tasks or scan_tasks[task_id]['status'] != TASK_RUNNING:
                        # 任务被取消
                        app.logger.info(f"扫描任务 {task_id} 被取消")
                        return

                    batch = stock_list[i:i + batch_size]
                    batch_results = []

                    for stock_code in batch:
                        try:
                            # 调用 quick_analyze_stock 并传入日期参数和强制刷新参数
                            if start_date or end_date:
                                report = analyzer.quick_analyze_stock(stock_code, market_type, start_date=start_date, end_date=end_date, force_refresh=force_refresh)
                            else:
                                report = analyzer.quick_analyze_stock(stock_code, market_type, force_refresh=force_refresh)
                            if report['score'] >= min_score:
                                batch_results.append(report)
                        except Exception as e:
                            app.logger.error(f"分析股票 {stock_code} 时出错: {str(e)}")
                            continue

                    results.extend(batch_results)

                    # 更新进度
                    progress = min(100, int((i + len(batch)) / total * 100))
                    start_market_scan_task_status(task_id, TASK_RUNNING, progress=progress)

                # 按得分排序
                results.sort(key=lambda x: x['score'], reverse=True)

                # 更新任务状态为完成
                start_market_scan_task_status(task_id, TASK_COMPLETED, progress=100, result=results)
                app.logger.info(f"扫描任务 {task_id} 完成，找到 {len(results)} 只符合条件的股票")

            except Exception as e:
                app.logger.error(f"扫描任务 {task_id} 失败: {str(e)}")
                app.logger.error(traceback.format_exc())
                start_market_scan_task_status(task_id, TASK_FAILED, error=str(e))

        # 启动后台线程
        thread = threading.Thread(target=run_scan)
        thread.daemon = True
        thread.start()

        return jsonify({
            'task_id': task_id,
            'status': 'pending',
            'message': f'已启动扫描任务，正在处理 {len(stock_list)} 只股票'
        })

    except Exception as e:
        app.logger.error(f"启动市场扫描任务时出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scan_status/<task_id>', methods=['GET'])
def get_scan_status(task_id):
    """获取扫描任务状态"""
    with task_lock:
        if task_id not in scan_tasks:
            return jsonify({'error': '找不到指定的扫描任务'}), 404

        task = scan_tasks[task_id]

        # 基本状态信息
        status = {
            'id': task['id'],
            'status': task['status'],
            'progress': task.get('progress', 0),
            'total': task.get('total', 0),
            'created_at': task['created_at'],
            'updated_at': task['updated_at']
        }

        # 如果任务完成，包含结果
        if task['status'] == TASK_COMPLETED and 'result' in task:
            status['result'] = task['result']

        # 如果任务失败，包含错误信息
        if task['status'] == TASK_FAILED and 'error' in task:
            status['error'] = task['error']

        return custom_jsonify(status)


@app.route('/api/cancel_scan/<task_id>', methods=['POST'])
def cancel_scan(task_id):
    """取消扫描任务"""
    with task_lock:
        if task_id not in scan_tasks:
            return jsonify({'error': '找不到指定的扫描任务'}), 404

        task = scan_tasks[task_id]

        if task['status'] in [TASK_COMPLETED, TASK_FAILED]:
            return jsonify({'message': '任务已完成或失败，无法取消'})

        # 更新状态为失败
        task['status'] = TASK_FAILED
        task['error'] = '用户取消任务'
        task['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        return jsonify({'message': '任务已取消'})


@app.route('/api/index_stocks', methods=['GET'])
def get_index_stocks():
    """获取指数成分股"""
    try:
        import akshare as ak
        index_code = request.args.get('index_code', '000300')  # 默认沪深300

        # 获取指数成分股
        app.logger.info(f"获取指数 {index_code} 成分股")
        if index_code == '000300':
            # 沪深300成分股
            stocks = get_index_stocks_with_fallback(index_code, "沪深300")
        elif index_code == '000905':
            # 中证500成分股
            stocks = get_index_stocks_with_fallback(index_code, "中证500")
        elif index_code == '000852':
            # 中证1000成分股
            stocks = get_index_stocks_with_fallback(index_code, "中证1000")
        elif index_code == '000001':
            # 上证指数
            stocks = get_index_stocks_with_fallback(index_code, "上证指数")
        elif index_code == 'market_cap_150':
            # 市值150亿以上的股票
            app.logger.info("获取市值150亿以上的股票")
            stocks = get_stocks_by_market_cap(min_market_cap=150)
        elif index_code == '399001':
            # 深证成指 - 尝试多种真实接口获取数据
            app.logger.info("获取深证成指成分股")
            success = False
            
            # 方法1：尝试概念板块接口
            try:
                app.logger.info("尝试概念板块接口获取深证成指")
                stocks = ak.stock_board_concept_cons_em(symbol="深证成指")
                if not stocks.empty:
                    app.logger.info(f"概念板块接口成功获取深证成指成分股: {len(stocks)}只")
                    success = True
            except Exception as e:
                app.logger.warning(f"概念板块接口失败: {str(e)}")
            
            # 方法2：尝试行业板块接口
            if not success:
                try:
                    app.logger.info("尝试行业板块接口获取深证成指")
                    stocks = ak.stock_board_industry_cons_em(symbol="深证成指")
                    if not stocks.empty:
                        app.logger.info(f"行业板块接口成功获取深证成指成分股: {len(stocks)}只")
                        success = True
                except Exception as e:
                    app.logger.warning(f"行业板块接口失败: {str(e)}")
            
            # 方法3：尝试实时行业资金流接口获取
            if not success:
                try:
                    app.logger.info("尝试实时行业资金流接口")
                    stocks = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="概念板块")
                    # 筛选包含"深证"相关的板块
                    if not stocks.empty and '板块名称' in stocks.columns:
                        stocks = stocks[stocks['板块名称'].str.contains('深证', na=False)]
                        if not stocks.empty:
                            app.logger.info(f"找到深证相关板块数据")
                            success = True
                except Exception as e:
                    app.logger.warning(f"实时行业资金流接口失败: {str(e)}")
            
            # 如果所有接口都失败，返回错误而不是预定义列表
            if not success:
                app.logger.error("所有深证成指接口都失败，无法获取实时数据")
                return jsonify({'error': '暂时无法获取深证成指成分股数据，请稍后重试'}), 503
                
        elif index_code == '399006':
            # 创业板指 - 尝试多种真实接口获取数据
            app.logger.info("获取创业板指成分股")
            success = False
            
            # 方法1：尝试概念板块接口
            try:
                app.logger.info("尝试概念板块接口获取创业板指")
                stocks = ak.stock_board_concept_cons_em(symbol="创业板")
                if not stocks.empty:
                    app.logger.info(f"概念板块接口成功获取创业板指成分股: {len(stocks)}只")
                    success = True
            except Exception as e:
                app.logger.warning(f"概念板块接口失败: {str(e)}")
            
            # 方法2：尝试行业板块接口
            if not success:
                try:
                    app.logger.info("尝试行业板块接口获取创业板指")
                    stocks = ak.stock_board_industry_cons_em(symbol="创业板")
                    if not stocks.empty:
                        app.logger.info(f"行业板块接口成功获取创业板指成分股: {len(stocks)}只")
                        success = True
                except Exception as e:
                    app.logger.warning(f"行业板块接口失败: {str(e)}")
            
            # 方法3：尝试获取创业板股票列表
            if not success:
                try:
                    app.logger.info("尝试获取创业板股票列表")
                    # 获取创业板股票（代码以300开头）
                    all_stocks = ak.stock_zh_a_spot_em()  # 获取所有A股
                    if not all_stocks.empty and '代码' in all_stocks.columns:
                        gem_stocks = all_stocks[all_stocks['代码'].str.startswith('300')]
                        if not gem_stocks.empty:
                            # 转换为类似其他接口的格式
                            stocks = gem_stocks[['代码', '名称']].copy()
                            stocks.columns = ['代码', '股票简称']
                            app.logger.info(f"获取到创业板股票: {len(stocks)}只")
                            success = True
                except Exception as e:
                    app.logger.warning(f"获取创业板股票列表失败: {str(e)}")
            
            # 如果所有接口都失败，返回错误而不是预定义列表
            if not success:
                app.logger.error("所有创业板指接口都失败，无法获取实时数据")
                return jsonify({'error': '暂时无法获取创业板指成分股数据，请稍后重试'}), 503
        else:
            return jsonify({'error': '不支持的指数代码'}), 400

        # 提取股票代码列表，尝试多种可能的列名
        stock_list = []
        if stocks is not None and not stocks.empty:
            app.logger.info(f"指数 {index_code} 数据列名: {stocks.columns.tolist()}")
            
            # 尝试不同的列名
            possible_code_columns = ['成分券代码', '代码', 'code', 'symbol', '股票代码', 'stock_code']
            
            for col in possible_code_columns:
                if col in stocks.columns:
                    stock_list = stocks[col].dropna().astype(str).str.strip().tolist()
                    app.logger.info(f"使用列 '{col}' 提取到 {len(stock_list)} 个股票代码")
                    break
            
            # 如果没找到明确的代码列，使用第一列
            if not stock_list and len(stocks.columns) > 0:
                first_col = stocks.columns[0]
                stock_list = stocks[first_col].dropna().astype(str).str.strip().tolist()
                app.logger.info(f"使用第一列 '{first_col}' 提取到 {len(stock_list)} 个股票代码")
            
            # 过滤掉无效的股票代码
            stock_list = [code for code in stock_list if code and code != 'nan' and code != '' and len(code) > 0]
            
        app.logger.info(f"找到 {len(stock_list)} 只 {index_code} 成分股")

        return jsonify({'stock_list': stock_list})
    except Exception as e:
        app.logger.error(f"获取指数成分股时出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/industry_stocks', methods=['GET'])
def get_industry_stocks():
    """获取概念/行业成分股"""
    try:
        import akshare as ak
        industry = request.args.get('industry', '')

        if not industry:
            return jsonify({'error': '请提供概念/行业名称'}), 400

        app.logger.info(f"获取概念/行业成分股，参数: {industry}")
        
        # 尝试多个可能的接口获取成分股数据
        stocks = None
        success = False
        
        # 方法1：尝试东方财富概念成分股接口
        try:
            app.logger.info(f"尝试使用东方财富概念成分股接口: {industry}")
            stocks = ak.stock_board_concept_cons_em(symbol=industry)
            if not stocks.empty:
                app.logger.info(f"东方财富概念接口成功获取 {industry} 成分股")
                app.logger.info(f"成分股数据列名: {stocks.columns.tolist()}")
                success = True
            else:
                app.logger.warning(f"东方财富概念接口返回空数据: {industry}")
        except Exception as e:
            app.logger.warning(f"东方财富概念接口失败: {str(e)}")
        
        # 方法2：如果概念接口失败，尝试行业接口
        if not success:
            try:
                app.logger.info(f"尝试使用东方财富行业接口: {industry}")
                stocks = ak.stock_board_industry_cons_em(symbol=industry)
                if not stocks.empty:
                    app.logger.info(f"东方财富行业接口成功获取 {industry} 成分股")
                    app.logger.info(f"成分股数据列名: {stocks.columns.tolist()}")
                    success = True
                else:
                    app.logger.warning(f"东方财富行业接口返回空数据: {industry}")
            except Exception as e:
                app.logger.warning(f"东方财富行业接口失败: {str(e)}")
        
        # 方法3：尝试同花顺概念板块成分股（如果存在）
        if not success:
            try:
                app.logger.info(f"尝试使用同花顺概念板块成分股接口: {industry}")
                # 注意：检查接口是否存在
                if hasattr(ak, 'stock_board_concept_cons_ths'):
                    stocks = ak.stock_board_concept_cons_ths(symbol=industry)
                elif hasattr(ak, 'stock_board_cons_ths'):
                    stocks = ak.stock_board_cons_ths(symbol=industry)
                else:
                    raise AttributeError("同花顺相关接口不存在")
                    
                if not stocks.empty:
                    app.logger.info(f"同花顺接口成功获取 {industry} 成分股")
                    success = True
                else:
                    app.logger.warning(f"同花顺接口返回空数据: {industry}")
            except Exception as e:
                app.logger.warning(f"同花顺概念接口失败: {str(e)}")
        
        # 如果所有方法都失败
        if not success or stocks is None or stocks.empty:
            raise Exception(f"无法获取 {industry} 的成分股数据，已尝试多个数据源但都失败或返回空数据")

        # 提取股票代码列表，尝试多种可能的列名
        stock_list = []
        
        try:
            if stocks is not None and not stocks.empty:
                possible_code_columns = ['代码', 'code', 'symbol', '股票代码']
                
                for col in possible_code_columns:
                    if col in stocks.columns:
                        # 安全地提取股票代码
                        stock_list = stocks[col].dropna().astype(str).str.strip().tolist()
                        app.logger.info(f"使用列 '{col}' 提取到 {len(stock_list)} 个股票代码")
                        break
                
                if not stock_list and len(stocks.columns) > 0:
                    # 如果没找到明确的代码列，使用第一列
                    first_col = stocks.columns[0]
                    stock_list = stocks[first_col].dropna().astype(str).str.strip().tolist()
                    app.logger.info(f"使用第一列 '{first_col}' 提取到 {len(stock_list)} 个股票代码")
                
                # 过滤掉无效的股票代码
                stock_list = [code for code in stock_list if code and code != 'nan' and code != '' and len(code) > 0]
                app.logger.info(f"过滤后有效股票代码数量: {len(stock_list)}")
            else:
                app.logger.error("股票数据为空")
                
        except Exception as e:
            app.logger.error(f"提取股票代码时出错: {str(e)}")
            stock_list = []
        
        app.logger.info(f"找到 {len(stock_list)} 只 {industry} 概念/行业股票")

        return jsonify({'stock_list': stock_list})
    except Exception as e:
        app.logger.error(f"获取概念/行业成分股时出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/concept_names', methods=['GET'])
def get_concept_names():
    """获取同花顺概念板块名称列表"""
    try:
        import akshare as ak
        
        app.logger.info("开始获取同花顺概念板块名称")
        
        # 尝试获取概念板块数据，使用多个接口
        concept_names = None
        options = []
        
        # 方法1：尝试东方财富概念板块名称
        try:
            app.logger.info("尝试使用东方财富概念板块名称接口")
            concept_names = ak.stock_board_concept_name_em()
            if concept_names is not None and not concept_names.empty:
                app.logger.info(f"东方财富概念名称接口成功，数据形状: {concept_names.shape}")
                app.logger.info(f"概念板块数据列名: {concept_names.columns.tolist()}")
        except Exception as e:
            app.logger.warning(f"东方财富概念名称接口失败: {str(e)}")
            concept_names = None
        
        # 如果成功获取到数据，处理数据
        if concept_names is not None and not concept_names.empty:
            try:
                app.logger.info(f"开始处理概念数据，数据形状: {concept_names.shape}")
                
                # 检查数据结构并正确提取概念名称和代码
                if len(concept_names.columns) >= 3:
                    # 第2列（索引1）：概念名称，第3列（索引2）：概念代码
                    name_col = concept_names.columns[1]  # 概念名称
                    code_col = concept_names.columns[2]  # 概念代码
                    app.logger.info(f"使用列: 名称列={name_col}, 代码列={code_col}")
                    
                    # 同时提取名称和代码
                    for _, row in concept_names.iterrows():
                        try:
                            concept_name = str(row[name_col]).strip()
                            concept_code = str(row[code_col]).strip()
                            
                            if (concept_name and concept_name != 'nan' and concept_name != '' and 
                                concept_code and concept_code != 'nan' and concept_code != ''):
                                options.append({'value': concept_code, 'label': concept_name})
                        except Exception as e:
                            app.logger.warning(f"处理概念数据行时出错: {e}")
                            continue
                            
                elif len(concept_names.columns) >= 2:
                    # 如果只有2列，可能是名称和代码，或者只有名称
                    name_col = concept_names.columns[1]
                    app.logger.info(f"只有2列，使用第2列作为概念名称: {name_col}")
                    
                    concept_list = concept_names[name_col].dropna().astype(str).str.strip().tolist()
                    
                    for concept_name in concept_list:
                        if concept_name and concept_name != 'nan' and concept_name != '' and len(concept_name) > 0:
                            # 如果没有代码列，使用名称作为value（向后兼容）
                            options.append({'value': concept_name, 'label': concept_name})
                            
                else:
                    app.logger.warning("概念数据列数不足，无法处理")
                    
                app.logger.info(f"成功处理 {len(options)} 个概念")
            except Exception as e:
                app.logger.error(f"处理概念数据时出错: {str(e)}")
                options = []
        else:
            app.logger.warning("无法获取概念板块数据，将使用默认选项")
        
        app.logger.info(f"成功获取 {len(options)} 个概念板块")
        app.logger.info(f"前3个概念示例: {options[:3]}")
        
        return jsonify({'concepts': options})
        
    except Exception as e:
        app.logger.error(f"获取概念板块名称时出错: {traceback.format_exc()}")
        # 返回默认选项作为后备（使用常见的概念，value使用概念名称作为向后兼容）
        default_options = [
            {'value': '半导体', 'label': '半导体'},
            {'value': '仪器仪表', 'label': '仪器仪表'},
            {'value': '医药', 'label': '医药'},
            {'value': '食品饮料', 'label': '食品饮料'},
            {'value': '新能源', 'label': '新能源'},
            {'value': '电池', 'label': '电池'},
            {'value': '电子元件', 'label': '电子元件'},
            {'value': '计算机', 'label': '计算机'},
            {'value': '互联网服务', 'label': '互联网服务'},
            {'value': '银行', 'label': '银行'},
            {'value': '非金属材料', 'label': '非金属材料'},
            {'value': '交运设备', 'label': '交运设备'},
            {'value': '人工智能', 'label': '人工智能'},
            {'value': '5G概念', 'label': '5G概念'},
            {'value': '新材料', 'label': '新材料'},
        ]
        app.logger.warning(f'获取最新概念数据失败，使用默认选项: {str(e)}')
        return jsonify({'concepts': default_options, 'error': f'获取最新数据失败，使用默认选项'})


# 添加到web_server.py
def clean_old_tasks():
    """清理旧的扫描任务"""
    with task_lock:
        now = datetime.now()
        to_delete = []

        for task_id, task in scan_tasks.items():
            # 解析更新时间
            try:
                updated_at = datetime.strptime(task['updated_at'], '%Y-%m-%d %H:%M:%S')
                # 如果任务完成或失败且超过1小时，或者任务状态异常且超过3小时，清理它
                if ((task['status'] in [TASK_COMPLETED, TASK_FAILED] and
                     (now - updated_at).total_seconds() > 3600) or
                        ((now - updated_at).total_seconds() > 10800)):
                    to_delete.append(task_id)
            except:
                # 日期解析错误，添加到删除列表
                to_delete.append(task_id)

        # 删除旧任务
        for task_id in to_delete:
            del scan_tasks[task_id]

        return len(to_delete)


# 修改 run_task_cleaner 函数，使其每 5 分钟运行一次并在 16:30 左右清理所有缓存
def run_task_cleaner():
    """定期运行任务清理，并在每天 16:30 左右清理所有缓存"""
    while True:
        try:
            now = datetime.now()
            # 判断是否在收盘时间附近（16:25-16:35）
            is_market_close_time = (now.hour == 16 and 25 <= now.minute <= 35)

            cleaned = clean_old_tasks()

            # 如果是收盘时间，清理所有缓存
            if is_market_close_time:
                # 清理分析器的数据缓存
                analyzer.data_cache.clear()

                # 清理 Flask 缓存
                cache.clear()

                # 清理任务存储
                with task_lock:
                    for task_type in tasks:
                        task_store = tasks[task_type]
                        completed_tasks = [task_id for task_id, task in task_store.items()
                                           if task['status'] == TASK_COMPLETED]
                        for task_id in completed_tasks:
                            del task_store[task_id]

                app.logger.info("市场收盘时间检测到，已清理所有缓存数据")

            if cleaned > 0:
                app.logger.info(f"清理了 {cleaned} 个旧的扫描任务")
        except Exception as e:
            app.logger.error(f"任务清理出错: {str(e)}")

        # 每 5 分钟运行一次，而不是每小时
        time.sleep(600)


# 基本面分析路由
@app.route('/api/fundamental_analysis', methods=['POST'])
def api_fundamental_analysis():
    try:
        data = request.json
        stock_code = data.get('stock_code')

        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400

        # 获取基本面分析结果
        result = fundamental_analyzer.calculate_fundamental_score(stock_code)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"基本面分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 资金流向分析路由
# Add to web_server.py

# API endpoint to get concept fund flow
@app.route('/api/concept_fund_flow', methods=['GET'])
def api_concept_fund_flow():
    try:
        period = request.args.get('period', '10日排行')  # Default to 10-day ranking

        # Get concept fund flow data
        result = capital_flow_analyzer.get_concept_fund_flow(period)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"Error getting concept fund flow: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# API endpoint to get individual stock fund flow ranking
@app.route('/api/individual_fund_flow_rank', methods=['GET'])
def api_individual_fund_flow_rank():
    try:
        period = request.args.get('period', '10日')  # Default to today

        # Get individual fund flow ranking data
        result = capital_flow_analyzer.get_individual_fund_flow_rank(period)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"Error getting individual fund flow ranking: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# API endpoint to get individual stock fund flow
@app.route('/api/individual_fund_flow', methods=['GET'])
def api_individual_fund_flow():
    try:
        stock_code = request.args.get('stock_code')
        market_type = request.args.get('market_type', '')  # Auto-detect if not provided
        re_date = request.args.get('period-select')

        if not stock_code:
            return jsonify({'error': 'Stock code is required'}), 400

        # Get individual fund flow data
        result = capital_flow_analyzer.get_individual_fund_flow(stock_code, market_type, re_date)
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"Error getting individual fund flow: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# API endpoint to get stocks in a sector
@app.route('/api/sector_stocks', methods=['GET'])
def api_sector_stocks():
    try:
        sector = request.args.get('sector')

        if not sector:
            return jsonify({'error': 'Sector name is required'}), 400

        # Get sector stocks data
        result = capital_flow_analyzer.get_sector_stocks(sector)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"Error getting sector stocks: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# Update the existing capital flow API endpoint
@app.route('/api/capital_flow', methods=['POST'])
def api_capital_flow():
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', '')  # Auto-detect if not provided

        if not stock_code:
            return jsonify({'error': 'Stock code is required'}), 400

        # Calculate capital flow score
        result = capital_flow_analyzer.calculate_capital_flow_score(stock_code, market_type)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"Error calculating capital flow score: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 情景预测路由
@app.route('/api/scenario_predict', methods=['POST'])
def api_scenario_predict():
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')
        days = data.get('days', 60)

        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400

        # 获取情景预测结果
        result = scenario_predictor.generate_scenarios(stock_code, market_type, days)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"情景预测出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 智能问答路由
@app.route('/api/qa', methods=['POST'])
def api_qa():
    try:
        data = request.json
        stock_code = data.get('stock_code')
        question = data.get('question')
        market_type = data.get('market_type', 'A')

        if not stock_code or not question:
            return jsonify({'error': '请提供股票代码和问题'}), 400

        # 获取智能问答结果
        result = stock_qa.answer_question(stock_code, question, market_type)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"智能问答出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 风险分析路由
@app.route('/api/risk_analysis', methods=['POST'])
def api_risk_analysis():
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')

        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400

        # 获取风险分析结果
        result = risk_monitor.analyze_stock_risk(stock_code, market_type)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"风险分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 投资组合风险分析路由
@app.route('/api/portfolio_risk', methods=['POST'])
def api_portfolio_risk():
    try:
        data = request.json
        portfolio = data.get('portfolio', [])

        if not portfolio:
            return jsonify({'error': '请提供投资组合'}), 400

        # 获取投资组合风险分析结果
        result = risk_monitor.analyze_portfolio_risk(portfolio)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"投资组合风险分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 指数分析路由
@app.route('/api/index_analysis', methods=['GET'])
def api_index_analysis():
    try:
        index_code = request.args.get('index_code')
        limit = int(request.args.get('limit', 30))

        if not index_code:
            return jsonify({'error': '请提供指数代码'}), 400

        # 获取指数分析结果
        result = index_industry_analyzer.analyze_index(index_code, limit)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"指数分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 行业分析路由
@app.route('/api/industry_analysis', methods=['GET'])
def api_industry_analysis():
    try:
        industry = request.args.get('industry')
        limit = int(request.args.get('limit', 30))

        if not industry:
            return jsonify({'error': '请提供行业名称'}), 400

        # 获取行业分析结果
        result = index_industry_analyzer.analyze_industry(industry, limit)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"行业分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/industry_fund_flow', methods=['GET'])
def api_industry_fund_flow():
    """获取行业资金流向数据"""
    try:
        symbol = request.args.get('symbol', '即时')

        result = industry_analyzer.get_industry_fund_flow(symbol)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"获取行业资金流向数据出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/industry_detail', methods=['GET'])
def api_industry_detail():
    """获取行业详细信息"""
    try:
        industry = request.args.get('industry')

        if not industry:
            return jsonify({'error': '请提供行业名称'}), 400

        result = industry_analyzer.get_industry_detail(industry)

        app.logger.info(f"返回前 (result)：{result}")
        if not result:
            return jsonify({'error': f'未找到行业 {industry} 的详细信息'}), 404

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"获取行业详细信息出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 行业比较路由
@app.route('/api/industry_compare', methods=['GET'])
def api_industry_compare():
    try:
        limit = int(request.args.get('limit', 10))

        # 获取行业比较结果
        result = index_industry_analyzer.compare_industries(limit)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"行业比较出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# CAN SLIM（欧奈尔）评分路由
@app.route('/api/can_slim_score', methods=['POST'])
def api_can_slim_score():
    """获取CAN SLIM（欧奈尔）评分"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')
        force_refresh = data.get('force_refresh', False)

        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400

        app.logger.info(f"计算股票 {stock_code} 的CAN SLIM评分（强制刷新: {force_refresh}）")

        # 计算CAN SLIM评分
        result = analyzer.calculate_can_slim_score(stock_code, market_type, force_refresh)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"计算CAN SLIM评分出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 清除CAN SLIM缓存路由
@app.route('/api/clear_can_slim_cache', methods=['POST'])
def api_clear_can_slim_cache():
    """清除CAN SLIM缓存"""
    try:
        data = request.json or {}
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')

        app.logger.info(f"清除CAN SLIM缓存: {stock_code if stock_code else '全部'}")

        # 清除缓存
        result = analyzer.clear_can_slim_cache(stock_code, market_type)

        return jsonify({
            'success': result,
            'message': f"缓存已清除: {stock_code if stock_code else '全部缓存'}"
        })
    except Exception as e:
        app.logger.error(f"清除CAN SLIM缓存出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 获取CAN SLIM缓存信息路由
@app.route('/api/can_slim_cache_info', methods=['GET'])
def api_can_slim_cache_info():
    """获取CAN SLIM缓存信息"""
    try:
        stock_code = request.args.get('stock_code')
        market_type = request.args.get('market_type', 'A')

        app.logger.info(f"获取CAN SLIM缓存信息: {stock_code if stock_code else '全部'}")

        # 获取缓存信息
        cache_info = analyzer.get_can_slim_cache_info(stock_code, market_type)

        return jsonify(cache_info)
    except Exception as e:
        app.logger.error(f"获取CAN SLIM缓存信息出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 市场扫描缓存管理API
@app.route('/api/clear_market_scan_cache', methods=['POST'])
def api_clear_market_scan_cache():
    """清除市场扫描缓存"""
    try:
        data = request.json or {}
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')
        index_code = data.get('index_code')

        app.logger.info(f"清除市场扫描缓存: stock_code={stock_code}, index_code={index_code}")

        # 清除缓存
        result = analyzer.clear_market_scan_cache(stock_code, market_type, index_code)

        return jsonify({
            'success': result,
            'message': f"市场扫描缓存已清除: {stock_code or index_code or '全部缓存'}"
        })
    except Exception as e:
        app.logger.error(f"清除市场扫描缓存出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/market_scan_cache_info', methods=['GET'])
def api_market_scan_cache_info():
    """获取市场扫描缓存信息"""
    try:
        stock_code = request.args.get('stock_code')
        market_type = request.args.get('market_type', 'A')

        app.logger.info(f"获取市场扫描缓存信息: {stock_code if stock_code else '全部'}")

        # 获取缓存信息
        cache_info = analyzer.get_market_scan_cache_info(stock_code, market_type)

        return jsonify(cache_info)
    except Exception as e:
        app.logger.error(f"获取市场扫描缓存信息出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 保存股票分析结果到数据库
def save_analysis_result(stock_code, market_type, result):
    """保存分析结果到数据库"""
    if not USE_DATABASE:
        return

    try:
        session = get_session()

        # 创建新的分析结果记录
        analysis = AnalysisResult(
            stock_code=stock_code,
            market_type=market_type,
            score=result.get('scores', {}).get('total', 0),
            recommendation=result.get('recommendation', {}).get('action', ''),
            technical_data=result.get('technical_analysis', {}),
            fundamental_data=result.get('fundamental_data', {}),
            capital_flow_data=result.get('capital_flow_data', {}),
            ai_analysis=result.get('ai_analysis', '')
        )

        session.add(analysis)
        session.commit()

    except Exception as e:
        app.logger.error(f"保存分析结果到数据库时出错: {str(e)}")
        if session:
            session.rollback()
    finally:
        if session:
            session.close()


# 从数据库获取历史分析结果
@app.route('/api/history_analysis', methods=['GET'])
def get_history_analysis():
    """获取股票的历史分析结果"""
    if not USE_DATABASE:
        return jsonify({'error': '数据库功能未启用'}), 400

    stock_code = request.args.get('stock_code')
    limit = int(request.args.get('limit', 10))

    if not stock_code:
        return jsonify({'error': '请提供股票代码'}), 400

    try:
        session = get_session()

        # 查询历史分析结果
        results = session.query(AnalysisResult) \
            .filter(AnalysisResult.stock_code == stock_code) \
            .order_by(AnalysisResult.analysis_date.desc()) \
            .limit(limit) \
            .all()

        # 转换为字典列表
        history = [result.to_dict() for result in results]

        return jsonify({'history': history})

    except Exception as e:
        app.logger.error(f"获取历史分析结果时出错: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if session:
            session.close()


# ======================== 数据同步相关API ========================

# 数据同步任务存储
sync_tasks = {}
sync_task_lock = threading.Lock()

def generate_sync_task_id():
    """生成同步任务ID"""
    import uuid
    return str(uuid.uuid4())

def update_sync_task_status(task_id, status, progress=None, result=None, error=None):
    """更新同步任务状态"""
    with sync_task_lock:
        if task_id in sync_tasks:
            task = sync_tasks[task_id]
            task['status'] = status
            if progress is not None:
                task['progress'] = progress
            if result is not None:
                task['result'] = result
            if error is not None:
                task['error'] = error
            task['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

@app.route('/api/start_data_sync', methods=['POST'])
def start_data_sync():
    """启动数据同步任务"""
    try:
        data = request.json
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        sync_type = data.get('sync_type', 'history')  # history, realtime
        
        if not start_date or not end_date:
            return jsonify({'error': '请提供开始和结束日期'}), 400
        
        # 创建同步任务
        task_id = generate_sync_task_id()
        task = {
            'id': task_id,
            'status': TASK_PENDING,
            'progress': 0,
            'sync_type': sync_type,
            'start_date': start_date,
            'end_date': end_date,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with sync_task_lock:
            sync_tasks[task_id] = task
        
        # 启动后台同步任务
        def run_sync():
            try:
                # 记录同步开始
                insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_RUNNING, progress=10)
                update_sync_task_status(task_id, TASK_RUNNING, progress=10)
                app.logger.info(f"开始数据同步任务 {task_id}, 类型: {sync_type}, 日期范围: {start_date} - {end_date}")
                
                success_records = 0
                failed_records = 0
                total_records = 0
                error_messages = []
                success_stock_codes = []  # 成功同步的股票代码列表
                failed_stock_codes = []   # 失败同步的股票代码列表
                
                if sync_type == 'realtime':
                    # 获取实时股票数据
                    update_sync_task_status(task_id, TASK_RUNNING, progress=20)
                    insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_RUNNING, progress=20)
                    
                    app.logger.info("正在获取实时股票数据...")
                    
                    # 获取实时数据
                    stock_data = ak.stock_zh_a_spot_em()
                    
                    total_records = len(stock_data) if stock_data is not None and not stock_data.empty else 0
                    
                    if not stock_data.empty:
                        update_sync_task_status(task_id, TASK_RUNNING, progress=40)
                        insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_RUNNING, 
                                            progress=40, total_records=total_records)
                        
                        # 1. 同步股票基本信息到stock_info表
                        app.logger.info(f"开始同步 {total_records} 条股票基本信息...")
                        info_success_count, info_failed_count, info_errors = insert_stock_info_to_db(stock_data)
                        app.logger.info(f"股票基本信息同步完成: 成功 {info_success_count} 条, 失败 {info_failed_count} 条")
                        
                        update_sync_task_status(task_id, TASK_RUNNING, progress=70)
                        
                        # 2. 同步股票实时数据到stock_realtime_data表
                        app.logger.info(f"开始同步 {total_records} 条实时数据...")
                        trade_date = datetime.now().strftime('%Y-%m-%d')
                        realtime_success_count, realtime_failed_count, realtime_errors = insert_stock_data_to_db(stock_data, trade_date)
                        app.logger.info(f"实时数据同步完成: 成功 {realtime_success_count} 条, 失败 {realtime_failed_count} 条")
                        
                        # 合并统计结果
                        total_success = info_success_count + realtime_success_count
                        total_failed = info_failed_count + realtime_failed_count
                        error_messages.extend(info_errors)
                        error_messages.extend(realtime_errors)
                        
                        update_sync_task_status(task_id, TASK_RUNNING, progress=90)
                        
                        result_message = f'成功同步股票信息 {info_success_count} 条, 实时数据 {realtime_success_count} 条'
                        if total_failed > 0:
                            result_message += f', 失败 {total_failed} 条'
                        
                        result = {
                            'total_records': total_records,
                            'success_records': total_success,
                            'failed_records': total_failed,
                            'info_success': info_success_count,
                            'realtime_success': realtime_success_count,
                            'sync_type': sync_type,
                            'start_date': start_date,
                            'end_date': end_date,
                            'message': result_message
                        }
                        
                        # 记录完成状态到数据库
                        insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_COMPLETED, 
                                            progress=100, total_records=total_records, 
                                            success_records=total_success, failed_records=total_failed,
                                            message=result_message, 
                                            error_details='; '.join(error_messages) if error_messages else None)
                        
                        update_sync_task_status(task_id, TASK_COMPLETED, progress=100, result=result)
                        app.logger.info(f"实时数据同步任务 {task_id} 完成: {result_message}")
                    else:
                        result_message = '未获取到实时数据'
                        result = {
                            'total_records': 0,
                            'success_records': 0,
                            'failed_records': 0,
                            'sync_type': sync_type,
                            'start_date': start_date,
                            'end_date': end_date,
                            'message': result_message
                        }
                        insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_COMPLETED, 
                                            progress=100, message=result_message)
                        update_sync_task_status(task_id, TASK_COMPLETED, progress=100, result=result)
                    
                else:
                    # 历史数据同步 - 使用 stock_zh_a_hist 接口
                    update_sync_task_status(task_id, TASK_RUNNING, progress=20)
                    insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_RUNNING, progress=20)
                    
                    app.logger.info("正在获取所有A股代码列表...")
                    
                    # 获取股票列表
                    stock_list = ak.stock_zh_a_spot_em()
                    if stock_list.empty:
                        raise Exception("获取到空的股票列表")
                    
                    # 1. 先同步股票基本信息列表
                    app.logger.info(f"开始同步 {len(stock_list)} 条股票基本信息...")
                    info_success_count, info_failed_count, info_errors = insert_stock_info_to_db(stock_list)
                    app.logger.info(f"股票基本信息同步完成: 成功 {info_success_count} 条, 失败 {info_failed_count} 条")
                    
                    update_sync_task_status(task_id, TASK_RUNNING, progress=30)
                    
                    # 2. 多线程并发同步历史数据
                    stock_codes = stock_list['代码'].tolist()
                    total_stocks = len(stock_codes)
                    
                    app.logger.info(f"开始多线程同步 {total_stocks} 只股票的历史数据...")
                    
                    # 使用多线程并发同步
                    success_records, failed_records, error_messages, success_stock_codes, failed_stock_codes = \
                        sync_stock_history_concurrent(task_id, sync_type, start_date, end_date, stock_codes, 
                                                    info_success_count, info_failed_count)
                    
                    # 合并统计结果
                    total_success = info_success_count + success_records
                    total_failed = info_failed_count + failed_records
                    total_records = total_success + total_failed
                    error_messages.extend(info_errors)
                    
                    result_message = f'历史数据同步完成: 股票信息 {info_success_count} 条, 历史数据 {success_records} 条'
                    if total_failed > 0:
                        result_message += f', 失败 {total_failed} 条'
                    
                    result = {
                        'total_records': total_records,
                        'success_records': total_success,
                        'failed_records': total_failed,
                        'info_success': info_success_count,
                        'history_success': success_records,
                        'sync_type': sync_type,
                        'start_date': start_date,
                        'end_date': end_date,
                        'message': result_message
                    }
                    
                    # 记录完成状态到数据库
                    insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_COMPLETED, 
                                        progress=100, total_records=total_records,
                                        success_records=total_success, failed_records=total_failed,
                                        message=result_message,
                                        error_details='; '.join(error_messages) if error_messages else None,
                                        success_stock_codes=success_stock_codes, 
                                        failed_stock_codes=failed_stock_codes)
                    
                    update_sync_task_status(task_id, TASK_COMPLETED, progress=100, result=result)
                    app.logger.info(f"历史数据同步任务 {task_id} 完成: {result_message}")
                
            except Exception as e:
                error_msg = f"同步任务失败: {str(e)}"
                app.logger.error(f"同步任务 {task_id} 失败: {str(e)}")
                
                # 记录失败状态到数据库
                insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_FAILED,
                                    message=error_msg, error_details=str(e))
                update_sync_task_status(task_id, TASK_FAILED, error=error_msg)
        
        # 启动后台线程
        thread = threading.Thread(target=run_sync)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'task_id': task_id,
            'status': 'pending',
            'message': f'已启动{sync_type}数据同步任务'
        })
        
    except Exception as e:
        app.logger.error(f"启动数据同步任务时出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/sync_status/<task_id>', methods=['GET'])
def get_sync_status(task_id):
    """获取同步任务状态"""
    with sync_task_lock:
        if task_id not in sync_tasks:
            return jsonify({'error': '找不到指定的同步任务'}), 404
        
        task = sync_tasks[task_id]
        
        status = {
            'id': task['id'],
            'status': task['status'],
            'progress': task.get('progress', 0),
            'sync_type': task.get('sync_type', 'realtime'),
            'start_date': task.get('start_date', ''),
            'end_date': task.get('end_date', ''),
            'created_at': task['created_at'],
            'updated_at': task['updated_at']
        }
        
        if task['status'] == TASK_COMPLETED and 'result' in task:
            status['result'] = task['result']
        
        if task['status'] == TASK_FAILED and 'error' in task:
            status['error'] = task['error']
        
        return custom_jsonify(status)

@app.route('/api/sync_history', methods=['GET'])
def get_sync_history():
    """获取同步历史记录"""
    try:
        # 从数据库中获取同步历史记录
        connection = get_mysql_connection()
        if not connection:
            # 如果数据库连接失败，从内存中获取
            with sync_task_lock:
                history = []
                for task_id, task in sync_tasks.items():
                    history.append({
                        'id': task['id'],
                        'sync_type': task.get('sync_type', 'history'),
                        'start_date': task.get('start_date', ''),
                        'end_date': task.get('end_date', ''),
                        'status': task['status'],
                        'progress': task.get('progress', 0),
                        'created_at': task['created_at'],
                        'updated_at': task['updated_at'],
                        'total_records': task.get('result', {}).get('total_records', 0) if task.get('result') else 0,
                        'message': task.get('result', {}).get('message', '') if task.get('result') else task.get('error', '')
                    })
                history.sort(key=lambda x: x['created_at'], reverse=True)
                return jsonify({'history': history})
        
        try:
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            
            # 查询同步历史记录，按创建时间倒序
            query_sql = """
            SELECT task_id as id, sync_type, start_date, end_date, status, progress,
                   total_records, success_records, failed_records, message,
                   success_stock_codes, failed_stock_codes,
                   DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') as created_at,
                   DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s') as updated_at
            FROM stock_sync_log 
            ORDER BY created_at DESC 
            LIMIT 50
            """
            
            cursor.execute(query_sql)
            db_records = cursor.fetchall()
            
            history = []
            for record in db_records:
                # 解析JSON格式的股票代码列表
                success_stock_codes = []
                failed_stock_codes = []
                
                if record['success_stock_codes']:
                    try:
                        success_stock_codes = json.loads(record['success_stock_codes'])
                    except (json.JSONDecodeError, TypeError):
                        success_stock_codes = []
                
                if record['failed_stock_codes']:
                    try:
                        failed_stock_codes = json.loads(record['failed_stock_codes'])
                    except (json.JSONDecodeError, TypeError):
                        failed_stock_codes = []
                
                history.append({
                    'id': record['id'],
                    'sync_type': record['sync_type'],
                    'start_date': record['start_date'].strftime('%Y-%m-%d') if record['start_date'] else '',
                    'end_date': record['end_date'].strftime('%Y-%m-%d') if record['end_date'] else '',
                    'status': record['status'],
                    'progress': record['progress'] or 0,
                    'created_at': record['created_at'],
                    'updated_at': record['updated_at'],
                    'total_records': record['total_records'] or 0,
                    'success_records': record['success_records'] or 0,
                    'failed_records': record['failed_records'] or 0,
                    'success_stock_codes': success_stock_codes,
                    'failed_stock_codes': failed_stock_codes,
                    'message': record['message'] or ''
                })
            
            # 如果数据库中没有记录，补充内存中的记录
            if not history:
                with sync_task_lock:
                    for task_id, task in sync_tasks.items():
                        history.append({
                            'id': task['id'],
                            'sync_type': task.get('sync_type', 'history'),
                            'start_date': task.get('start_date', ''),
                            'end_date': task.get('end_date', ''),
                            'status': task['status'],
                            'progress': task.get('progress', 0),
                            'created_at': task['created_at'],
                            'updated_at': task['updated_at'],
                            'total_records': task.get('result', {}).get('total_records', 0) if task.get('result') else 0,
                            'success_records': task.get('result', {}).get('success_records', 0) if task.get('result') else 0,
                            'failed_records': task.get('result', {}).get('failed_records', 0) if task.get('result') else 0,
                            'success_stock_codes': task.get('result', {}).get('success_stock_codes', []) if task.get('result') else [],
                            'failed_stock_codes': task.get('result', {}).get('failed_stock_codes', []) if task.get('result') else [],
                            'message': task.get('result', {}).get('message', '') if task.get('result') else task.get('error', '')
                        })
                    history.sort(key=lambda x: x['created_at'], reverse=True)
            
            return jsonify({'history': history})
            
        except Exception as e:
            app.logger.error(f"从数据库获取同步历史记录失败: {str(e)}")
            # 失败时从内存获取
            with sync_task_lock:
                history = []
                for task_id, task in sync_tasks.items():
                    history.append({
                        'id': task['id'],
                        'sync_type': task.get('sync_type', 'history'),
                        'start_date': task.get('start_date', ''),
                        'end_date': task.get('end_date', ''),
                        'status': task['status'],
                        'progress': task.get('progress', 0),
                        'created_at': task['created_at'],
                        'updated_at': task['updated_at'],
                        'total_records': task.get('result', {}).get('total_records', 0) if task.get('result') else 0,
                        'success_records': task.get('result', {}).get('success_records', 0) if task.get('result') else 0,
                        'failed_records': task.get('result', {}).get('failed_records', 0) if task.get('result') else 0,
                        'success_stock_codes': task.get('result', {}).get('success_stock_codes', []) if task.get('result') else [],
                        'failed_stock_codes': task.get('result', {}).get('failed_stock_codes', []) if task.get('result') else [],
                        'message': task.get('result', {}).get('message', '') if task.get('result') else task.get('error', '')
                    })
                history.sort(key=lambda x: x['created_at'], reverse=True)
                return jsonify({'history': history})
        finally:
            if connection:
                connection.close()
        
    except Exception as e:
        app.logger.error(f"获取同步历史记录时出错: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/cancel_sync/<task_id>', methods=['POST'])
def cancel_sync_task(task_id):
    """取消同步任务"""
    with sync_task_lock:
        if task_id not in sync_tasks:
            return jsonify({'error': '找不到指定的同步任务'}), 404
        
        task = sync_tasks[task_id]
        
        if task['status'] in [TASK_COMPLETED, TASK_FAILED]:
            return jsonify({'message': '任务已完成或失败，无法取消'})
        
        # 更新状态为失败
        task['status'] = TASK_FAILED
        task['error'] = '用户取消任务'
        task['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({'message': '任务已取消'})

@app.route('/api/check_task_status/<task_id>', methods=['GET'])
def check_task_in_memory(task_id):
    """检查任务是否在内存中存在"""
    with sync_task_lock:
        exists_in_memory = task_id in sync_tasks
        
        result = {
            'task_id': task_id,
            'exists_in_memory': exists_in_memory
        }
        
        if exists_in_memory:
            task = sync_tasks[task_id]
            result.update({
                'memory_status': task['status'],
                'memory_progress': task.get('progress', 0),
                'last_updated': task['updated_at']
            })
            
            # 如果内存中的状态是completed，同时更新数据库状态
            if task['status'] == 'completed':
                try:
                    connection = get_mysql_connection()
                    if connection:
                        cursor = connection.cursor()
                        # 检查数据库中的状态是否已经是completed
                        check_sql = "SELECT status FROM stock_sync_log WHERE task_id = %s"
                        cursor.execute(check_sql, (task_id,))
                        db_result = cursor.fetchone()
                        
                        if db_result and db_result[0] != 'completed':
                            # 获取内存中的任务结果数据
                            task_result = task.get('result', {})
                            
                            update_sql = """
                            UPDATE stock_sync_log 
                            SET status = 'completed', 
                                progress = %s,
                                total_records = %s,
                                success_records = %s,
                                failed_records = %s,
                                message = %s,
                                updated_at = CURRENT_TIMESTAMP,
                                end_time = CURRENT_TIMESTAMP
                            WHERE task_id = %s
                            """
                            cursor.execute(update_sql, (
                                task.get('progress', 100),
                                task_result.get('total_records', 0),
                                task_result.get('success_records', 0),
                                task_result.get('failed_records', 0),
                                task_result.get('message', '同步完成'),
                                task_id
                            ))
                            affected_rows = cursor.rowcount
                            connection.commit()
                            
                            result['database_updated'] = affected_rows > 0
                            if affected_rows > 0:
                                app.logger.info(f"任务 {task_id} 内存状态为completed，已同步更新数据库状态")
                        else:
                            result['database_updated'] = False
                            result['database_already_completed'] = True
                        
                        cursor.close()
                        connection.close()
                        
                except Exception as e:
                    app.logger.error(f"更新任务 {task_id} 数据库状态失败: {str(e)}")
                    result['database_update_error'] = str(e)
        else:
            # 如果任务不在内存中，更新数据库状态为失败
            try:
                connection = get_mysql_connection()
                if connection:
                    cursor = connection.cursor()
                    update_sql = """
                    UPDATE stock_sync_log 
                    SET status = 'failed', 
                        message = '任务已从内存中丢失',
                        error_details = '检查发现任务不在服务器内存中，可能由于服务器重启或异常',
                        updated_at = CURRENT_TIMESTAMP,
                        end_time = CURRENT_TIMESTAMP
                    WHERE task_id = %s AND status = 'running'
                    """
                    cursor.execute(update_sql, (task_id,))
                    affected_rows = cursor.rowcount
                    connection.commit()
                    cursor.close()
                    connection.close()
                    
                    result['database_updated'] = affected_rows > 0
                    if affected_rows > 0:
                        app.logger.info(f"任务 {task_id} 不在内存中，已更新数据库状态为失败")
                    
            except Exception as e:
                app.logger.error(f"更新任务 {task_id} 数据库状态失败: {str(e)}")
                result['database_update_error'] = str(e)
        
        return jsonify(result)

@app.route('/api/continue_sync', methods=['POST'])
def continue_sync():
    """在原有任务上继续同步失败的股票代码"""
    try:
        data = request.json
        task_id = data.get('task_id')  # 直接使用原有任务ID
        
        if not task_id:
            return jsonify({'error': '请提供任务ID'}), 400
        
        # 从数据库中获取原始任务信息
        connection = get_mysql_connection()
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
        
        try:
            cursor = connection.cursor()
            cursor.execute("""
                SELECT task_id, sync_type, start_date, end_date, failed_stock_codes, success_stock_codes 
                FROM stock_sync_log 
                WHERE task_id = %s AND failed_stock_codes IS NOT NULL
            """, (task_id,))
            
            task_info = cursor.fetchone()
            if not task_info:
                return jsonify({'error': '找不到任务或任务没有失败的股票代码'}), 404
            
            task_id, sync_type, start_date, end_date, failed_stock_codes_json, success_stock_codes_json = task_info
            
            # 确保日期格式正确
            if isinstance(start_date, date):
                start_date = start_date.strftime('%Y-%m-%d')
            if isinstance(end_date, date):
                end_date = end_date.strftime('%Y-%m-%d')
            
            # 解析失败的股票代码
            if not failed_stock_codes_json:
                return jsonify({'error': '没有失败的股票代码需要重新同步'}), 400
            
            import json
            failed_stock_codes = json.loads(failed_stock_codes_json)
            success_stock_codes = json.loads(success_stock_codes_json) if success_stock_codes_json else []
            
            if not failed_stock_codes:
                return jsonify({'error': '没有失败的股票代码需要重新同步'}), 400
            
            # 更新原有任务状态为运行中
            update_sql = """
                UPDATE stock_sync_log 
                SET status = 'running', 
                    progress = 10,
                    message = '继续同步失败的股票...',
                    updated_at = CURRENT_TIMESTAMP
                WHERE task_id = %s
            """
            cursor.execute(update_sql, (task_id,))
            connection.commit()
            
            # 将任务加入内存（如果不存在）
            with sync_task_lock:
                if task_id not in sync_tasks:
                    sync_tasks[task_id] = {
                        'id': task_id,
                        'status': TASK_RUNNING,
                        'progress': 10,
                        'sync_type': sync_type,
                        'start_date': start_date,
                        'end_date': end_date,
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                else:
                    sync_tasks[task_id]['status'] = TASK_RUNNING
                    sync_tasks[task_id]['progress'] = 10
            
            # 启动后台重新同步任务
            def run_retry_sync():
                try:
                    insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_RUNNING, progress=10)
                    update_sync_task_status(task_id, TASK_RUNNING, progress=10)
                    app.logger.info(f"开始重新同步任务 {task_id}, 重试 {len(failed_stock_codes)} 只股票")
                    
                    success_records = 0
                    failed_records = 0
                    error_messages = []
                    total_stocks = len(failed_stock_codes)
                    app.logger.info(f"开始多线程重新同步 {total_stocks} 只失败股票...")
                    
                    # 使用多线程并发重新同步（使用较少线程避免对API压力过大）
                    success_records, failed_records, error_messages, success_stock_codes, new_failed_stock_codes = \
                        sync_stock_history_concurrent_retry(task_id, sync_type, start_date, end_date, failed_stock_codes,
                                                          task_id, max_workers=4)
                    
                    # 更新原始任务的成功和失败股票代码列表
                    if success_stock_codes or new_failed_stock_codes:
                        update_original_task_stock_codes(task_id, success_stock_codes, new_failed_stock_codes)
                    
                    result_message = f'重新同步完成: 成功 {len(success_stock_codes)} 只股票, 失败 {len(new_failed_stock_codes)} 只股票'
                    
                    result = {
                        'total_records': len(failed_stock_codes),
                        'success_records': success_records,
                        'failed_records': failed_records,
                        'retry_success_count': len(success_stock_codes),
                        'retry_failed_count': len(new_failed_stock_codes),
                        'sync_type': sync_type,
                        'start_date': start_date,
                        'end_date': end_date,
                        'original_task_id': task_id,
                        'message': result_message
                    }
                    
                    insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_COMPLETED, 
                                        progress=100, total_records=len(failed_stock_codes),
                                        success_records=success_records, failed_records=failed_records,
                                        message=result_message,
                                        error_details='; '.join(error_messages) if error_messages else None,
                                        success_stock_codes=success_stock_codes, 
                                        failed_stock_codes=new_failed_stock_codes)
                    
                    update_sync_task_status(task_id, TASK_COMPLETED, progress=100, result=result)
                    app.logger.info(f"重新同步任务 {task_id} 完成: {result_message}")
                
                except Exception as e:
                    error_msg = f"重新同步任务失败: {str(e)}"
                    app.logger.error(f"重新同步任务 {task_id} 失败: {str(e)}")
                    
                    insert_sync_log_to_db(task_id, sync_type, start_date, end_date, TASK_FAILED,
                                        message=error_msg, error_details=str(e))
                    update_sync_task_status(task_id, TASK_FAILED, error=error_msg)
            
            thread = threading.Thread(target=run_retry_sync)
            thread.daemon = True
            thread.start()
            
            return jsonify({
                'task_id': task_id,
                'status': 'pending',
                'message': f'已启动重新同步任务，将重试 {len(failed_stock_codes)} 只失败股票'
            })
            
        finally:
            cursor.close()
            connection.close()
        
    except Exception as e:
        app.logger.error(f"启动重新同步任务时出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

def update_original_task_stock_codes(original_task_id, success_codes, failed_codes):
    """更新原始任务的成功和失败股票代码列表"""
    import json
    
    connection = get_mysql_connection()
    if not connection:
        return
    
    try:
        cursor = connection.cursor()
        
        cursor.execute("""
            SELECT success_stock_codes, failed_stock_codes 
            FROM stock_sync_log 
            WHERE task_id = %s
        """, (original_task_id,))
        
        result = cursor.fetchone()
        if not result:
            return
        
        success_json, failed_json = result
        
        current_success = json.loads(success_json) if success_json else []
        current_failed = json.loads(failed_json) if failed_json else []
        
        current_success.extend(success_codes)
        
        for code in success_codes:
            if code in current_failed:
                current_failed.remove(code)
        
        current_failed = [code for code in current_failed if code not in success_codes]
        current_failed.extend(failed_codes)
        
        current_success = list(set(current_success))
        current_failed = list(set(current_failed))
        
        cursor.execute("""
            UPDATE stock_sync_log 
            SET success_stock_codes = %s, failed_stock_codes = %s, updated_at = CURRENT_TIMESTAMP
            WHERE task_id = %s
        """, (json.dumps(current_success), json.dumps(current_failed), original_task_id))
        
        connection.commit()
        app.logger.info(f"更新原始任务 {original_task_id} 的股票代码列表: 成功+{len(success_codes)}, 失败-{len(success_codes)}+{len(failed_codes)}")
        
    except Exception as e:
        app.logger.error(f"更新原始任务股票代码列表失败: {e}")
    finally:
        cursor.close()
        connection.close()

# ================== ML机器学习评分相关API ==================

@app.route('/api/ml_score', methods=['POST'])
def ml_score():
    """机器学习综合评分接口"""
    try:
        data = request.get_json()
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')
        
        if not stock_code:
            return jsonify({'success': False, 'message': '股票代码不能为空'})
        
        # 获取股票数据
        from datetime import datetime, timedelta
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        stock_data = analyzer.get_stock_data(stock_code, market_type=market_type, 
                                          start_date=start_date, end_date=end_date)
        if stock_data is None or stock_data.empty:
            return jsonify({'success': False, 'message': '无法获取股票数据'})
        
        # 获取基本面数据
        try:
            fundamental_data = fundamental_analyzer.get_fundamental_data(stock_code)
        except:
            fundamental_data = None
        
        # 获取市场环境数据
        try:
            market_data = {
                'sh_index': analyzer.get_stock_data('000001', start_date=start_date, end_date=end_date),
                'sz_index': analyzer.get_stock_data('399001', start_date=start_date, end_date=end_date)
            }
        except:
            market_data = None
        
        # 检查模型是否已训练
        if not hasattr(ml_scoring_system, 'models') or not ml_scoring_system.models:
            # 如果模型未训练，返回基于技术指标的简单评分
            features = ml_feature_engineer.extract_comprehensive_features(stock_data)
            if not features.empty:
                # 基于最新的特征计算简单评分
                latest_features = features.iloc[-1]
                simple_score = min(max((latest_features.mean() * 10), 0), 100)
                ml_result = {
                    'prediction': float(simple_score),
                    'confidence': 0.6,
                    'feature_importance': {},
                    'model_scores': {'simple_technical': float(simple_score)}
                }
            else:
                ml_result = {
                    'prediction': 50.0,
                    'confidence': 0.3,
                    'feature_importance': {},
                    'model_scores': {'default': 50.0}
                }
        else:
            # 使用已训练的ML系统进行评分
            # 将DataFrame转换为字典格式
            stock_data_dict = {
                'price_data': stock_data.to_dict('records'),
                'fundamental_data': fundamental_data,
                'market_data': market_data
            }
            ml_result = ml_scoring_system.predict_score(stock_data_dict)
        
        return jsonify({
            'success': True,
            'data': {
                'stock_code': stock_code,
                'ml_score': ml_result['prediction'],
                'confidence': ml_result['confidence'],
                'feature_importance': ml_result.get('feature_importance', {}),
                'model_scores': ml_result.get('model_scores', {}),
                'prediction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        })
        
    except Exception as e:
        app.logger.error(f"ML评分失败: {e}")
        return jsonify({'success': False, 'message': f'ML评分失败: {str(e)}'})

@app.route('/api/ml_batch_score', methods=['POST'])
def ml_batch_score():
    """批量ML评分接口"""
    try:
        data = request.get_json()
        stock_codes = data.get('stock_codes', [])
        market_type = data.get('market_type', 'A')
        
        if not stock_codes:
            return jsonify({'success': False, 'message': '股票代码列表不能为空'})
        
        results = []
        for stock_code in stock_codes:
            try:
                # 获取股票数据
                from datetime import datetime, timedelta
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
                stock_data = analyzer.get_stock_data(stock_code, market_type=market_type, 
                                                  start_date=start_date, end_date=end_date)
                if stock_data is None or stock_data.empty:
                    continue
                
                # 获取基本面数据
                try:
                    fundamental_data = fundamental_analyzer.get_fundamental_data(stock_code)
                except:
                    fundamental_data = None
                
                # 使用ML系统进行评分
                if hasattr(ml_scoring_system, 'models') and ml_scoring_system.models:
                    stock_data_dict = {
                        'price_data': stock_data.to_dict('records'),
                        'fundamental_data': fundamental_data,
                        'market_data': None
                    }
                    ml_result = ml_scoring_system.predict_score(stock_data_dict)
                else:
                    # 如果模型未训练，使用简单评分
                    features = ml_feature_engineer.extract_comprehensive_features(stock_data)
                    if not features.empty:
                        latest_features = features.iloc[-1]
                        simple_score = min(max((latest_features.mean() * 10), 0), 100)
                        ml_result = {
                            'prediction': float(simple_score),
                            'confidence': 0.6
                        }
                    else:
                        ml_result = {
                            'prediction': 50.0,
                            'confidence': 0.3
                        }
                    
                
                results.append({
                    'stock_code': stock_code,
                    'ml_score': ml_result['prediction'],
                    'confidence': ml_result['confidence']
                })
                
            except Exception as e:
                app.logger.warning(f"股票 {stock_code} ML评分失败: {e}")
                results.append({
                    'stock_code': stock_code,
                    'ml_score': 0,
                    'confidence': 0,
                    'error': str(e)
                })
        
        return jsonify({
            'success': True,
            'data': results,
            'total_count': len(results)
        })
        
    except Exception as e:
        app.logger.error(f"批量ML评分失败: {e}")
        return jsonify({'success': False, 'message': f'批量ML评分失败: {str(e)}'})

@app.route('/api/ml_train_model', methods=['POST'])
def ml_train_model():
    """训练ML模型接口"""
    try:
        data = request.get_json()
        training_data_path = data.get('training_data_path')
        model_name = data.get('model_name', 'ensemble')
        
        # 如果没有提供训练数据路径，使用默认的股票列表进行训练
        if not training_data_path:
            # 获取热门股票进行训练
            popular_stocks = ['000002', '000001', '600036', '000858', '002415']
            training_data = []
            
            for stock_code in popular_stocks:
                try:
                    from datetime import datetime, timedelta
                    end_date = datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y%m%d')
                    stock_data = analyzer.get_stock_data(stock_code, start_date=start_date, end_date=end_date)
                    if stock_data is not None and not stock_data.empty:
                        # 提取特征
                        features = ml_feature_engineer.extract_comprehensive_features(stock_data)
                        if not features.empty:
                            # 生成标签（基于未来收益率）
                            features['future_return'] = stock_data['close'].pct_change(5).shift(-5)
                            training_data.append(features.dropna())
                except Exception as e:
                    app.logger.warning(f"获取股票 {stock_code} 训练数据失败: {e}")
            
            if training_data:
                training_df = pd.concat(training_data, ignore_index=True)
                # 分离特征和标签
                feature_columns = [col for col in training_df.columns if col != 'future_return']
                X = training_df[feature_columns]
                y = training_df['future_return']
                
                # 训练模型
                ml_scoring_system.train_models(X, y)
                
                return jsonify({
                    'success': True,
                    'message': f'模型训练完成，训练样本数: {len(training_df)}',
                    'training_samples': len(training_df),
                    'features_count': len(feature_columns)
                })
            else:
                return jsonify({'success': False, 'message': '无法获取足够的训练数据'})
        else:
            return jsonify({'success': False, 'message': '自定义训练数据路径功能暂未实现'})
        
    except Exception as e:
        app.logger.error(f"模型训练失败: {e}")
        return jsonify({'success': False, 'message': f'模型训练失败: {str(e)}'})

@app.route('/api/ml_model_status', methods=['GET'])
def ml_model_status():
    """获取ML模型状态接口"""
    try:
        # 检查模型是否已训练
        models_trained = hasattr(ml_scoring_system, 'models') and bool(ml_scoring_system.models)
        
        # 获取在线学习状态
        online_status = ml_online_learning.is_running if hasattr(ml_online_learning, 'is_running') else False
        
        # 获取模型性能信息（只返回可序列化的数据）
        performance_info = {}
        if hasattr(ml_scoring_system, 'performance_metrics'):
            performance_info = ml_scoring_system.performance_metrics
        
        # 获取模型列表（只返回名称，不返回模型对象）
        model_names = list(ml_scoring_system.models.keys()) if models_trained else []
        
        return jsonify({
            'success': True,
            'data': {
                'models_trained': models_trained,
                'online_learning_active': online_status,
                'model_count': len(ml_scoring_system.models) if models_trained else 0,
                'model_names': model_names,
                'performance_metrics': performance_info,
                'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        })
        
    except Exception as e:
        app.logger.error(f"获取ML模型状态失败: {e}")
        return jsonify({'success': False, 'message': f'获取模型状态失败: {str(e)}'})

@app.route('/api/ml_start_online_learning', methods=['POST'])
def ml_start_online_learning():
    """启动在线学习接口"""
    try:
        ml_online_learning.start_online_learning()
        return jsonify({
            'success': True,
            'message': '在线学习系统启动成功'
        })
    except Exception as e:
        app.logger.error(f"启动在线学习失败: {e}")
        return jsonify({'success': False, 'message': f'启动在线学习失败: {str(e)}'})

@app.route('/api/ml_stop_online_learning', methods=['POST'])
def ml_stop_online_learning():
    """停止在线学习接口"""
    try:
        ml_online_learning.stop_online_learning()
        return jsonify({
            'success': True,
            'message': '在线学习系统停止成功'
        })
    except Exception as e:
        app.logger.error(f"停止在线学习失败: {e}")
        return jsonify({'success': False, 'message': f'停止在线学习失败: {str(e)}'})

# ================== End of ML APIs ==================

# 在应用启动时启动清理线程（保持原有代码不变）
cleaner_thread = threading.Thread(target=run_task_cleaner)
cleaner_thread.daemon = True
cleaner_thread.start()

if __name__ == '__main__':
    # 将 host 设置为 '0.0.0.0' 使其支持所有网络接口访问
    app.run(host='0.0.0.0', port=8888, debug=False)