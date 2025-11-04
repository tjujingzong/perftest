#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
归一化建模：将小机测试结果转换为可外推的单位指标
计算指标：
1. 单位核心吞吐（TPS/核心数、msg/s/核心数）
2. 单位内存足迹（内存占用/消息数、内存占用/事务数）
3. 单位消息/事务开销（延迟/消息大小、吞吐/资源占用等）
"""

import pandas as pd
import numpy as np
import argparse
import pathlib
from typing import Dict, List, Optional, Tuple
import sys
from datetime import datetime


class NormalizedMetrics:
    """归一化指标计算器"""
    
    def __init__(self, cpu_cores: int = 4, memory_gb: float = 4.0):
        """
        初始化归一化计算器
        
        Args:
            cpu_cores: 测试环境CPU核心数
            memory_gb: 测试环境内存大小（GB）
        """
        self.cpu_cores = cpu_cores
        self.memory_gb = memory_gb
        self.memory_bytes = memory_gb * 1024 * 1024 * 1024
    
    def normalize_db_metrics(self, df: pd.DataFrame, component_name: str = "KingbaseES") -> pd.DataFrame:
        """
        归一化数据库性能指标
        
        Args:
            df: 包含数据库测试结果的DataFrame
            component_name: 组件名称
            
        Returns:
            包含归一化指标的DataFrame
        """
        results = []
        
        for _, row in df.iterrows():
            if pd.isna(row.get('tps_excluding')) or row.get('return_code', 1) != 0:
                continue
            
            clients = row.get('clients', 0)
            jobs = row.get('jobs', 0)
            tps = row.get('tps_excluding', 0)
            latency_ms = row.get('latency_ms_avg', 0)
            
            if tps <= 0:
                continue
            
            # 单位核心吞吐（TPS/核心）
            tps_per_core = tps / self.cpu_cores
            
            # 单位客户端吞吐（TPS/客户端）
            tps_per_client = tps / clients if clients > 0 else 0
            
            # 单位线程吞吐（TPS/线程）
            tps_per_job = tps / jobs if jobs > 0 else 0
            
            # 单位事务延迟（ms/事务）
            latency_per_tx = latency_ms
            
            # 单位核心延迟（假设延迟与核心数相关）
            latency_per_core = latency_ms  # 延迟通常与核心数无关，但保留字段
            
            # 吞吐密度（TPS/GB内存）
            tps_per_gb = tps / self.memory_gb
            
            # 估算单位事务内存占用（假设内存占用与TPS相关）
            # 基于经验值：每个连接约2MB，加上缓存等
            estimated_mem_per_tx = (self.memory_bytes * 0.3) / (tps * 60) if tps > 0 else 0
            
            # 资源利用率估算（基于TPS和并发数）
            # CPU利用率 = (实际TPS / 理论最大TPS) * 100，理论值基于经验
            estimated_max_tps = self.cpu_cores * 500  # 假设每核心最大500 TPS
            cpu_utilization = min(100, (tps / estimated_max_tps) * 100) if estimated_max_tps > 0 else 0
            
            results.append({
                'component': component_name,
                'component_type': 'DB',
                'timestamp': row.get('timestamp', ''),
                'clients': clients,
                'jobs': jobs,
                'duration_s': row.get('duration_s', 0),
                
                # 原始指标
                'tps': tps,
                'latency_ms': latency_ms,
                'tx_processed': row.get('tx_processed', 0),
                
                # 归一化指标（单位核心）
                'tps_per_core': round(tps_per_core, 2),
                'latency_ms_per_core': round(latency_per_core, 2),
                
                # 归一化指标（单位资源）
                'tps_per_client': round(tps_per_client, 2),
                'tps_per_job': round(tps_per_job, 2),
                'tps_per_gb_memory': round(tps_per_gb, 2),
                
                # 单位事务开销
                'latency_per_tx_ms': round(latency_per_tx, 2),
                'memory_per_tx_bytes': round(estimated_mem_per_tx, 2),
                
                # 资源利用率
                'cpu_utilization_pct': round(cpu_utilization, 2),
                
                # 测试环境
                'test_cpu_cores': self.cpu_cores,
                'test_memory_gb': self.memory_gb,
            })
        
        return pd.DataFrame(results)
    
    def normalize_mq_metrics(self, summary_df: pd.DataFrame, component_name: str = "RabbitMQ") -> pd.DataFrame:
        """
        归一化消息队列性能指标
        
        Args:
            summary_df: 包含MQ测试汇总结果的DataFrame
            component_name: 组件名称
            
        Returns:
            包含归一化指标的DataFrame
        """
        results = []
        
        for _, row in summary_df.iterrows():
            if not row.get('success', False) or row.get('avg_received_msg_s', 0) <= 0:
                continue
            
            target_rate = row.get('target_rate_msg_s', 0)
            avg_sent = row.get('avg_sent_msg_s', 0)
            avg_received = row.get('avg_received_msg_s', 0)
            worst_p95 = row.get('worst_p95_ms', 0)
            producers = row.get('producers', 0)
            consumers = row.get('consumers', 0)
            size_bytes = row.get('size_bytes', 0)
            duration_s = row.get('duration_s', 0)
            
            # 单位核心吞吐（msg/s/核心）
            msg_per_sec_per_core = avg_received / self.cpu_cores
            
            # 单位生产者吞吐（msg/s/生产者）
            msg_per_sec_per_producer = avg_received / producers if producers > 0 else 0
            
            # 单位消费者吞吐（msg/s/消费者）
            msg_per_sec_per_consumer = avg_received / consumers if consumers > 0 else 0
            
            # 单位消息延迟（ms/消息）
            latency_per_msg = worst_p95
            
            # 吞吐密度（msg/s/GB内存）
            msg_per_sec_per_gb = avg_received / self.memory_gb
            
            # 单位消息大小吞吐（msg/s/KB）
            msg_per_sec_per_kb = avg_received / (size_bytes / 1024) if size_bytes > 0 else 0
            
            # 估算单位消息内存占用
            # 基于消息大小和队列长度估算
            estimated_mem_per_msg = size_bytes * 1.5  # 消息本身 + 开销
            
            # 吞吐带宽（MB/s）
            throughput_mbps = (avg_received * size_bytes) / (1024 * 1024)
            
            # CPU利用率估算
            # 假设每核心最大处理能力为10000 msg/s
            estimated_max_msg_per_sec = self.cpu_cores * 10000
            cpu_utilization = min(100, (avg_received / estimated_max_msg_per_sec) * 100) if estimated_max_msg_per_sec > 0 else 0
            
            # 消息丢失率
            loss_ratio = 1 - (avg_received / avg_sent) if avg_sent > 0 else 0
            
            results.append({
                'component': component_name,
                'component_type': 'MQ',
                'run_id': row.get('run_id', ''),
                'target_rate_msg_s': target_rate,
                'duration_s': duration_s,
                
                # 原始指标
                'avg_sent_msg_s': avg_sent,
                'avg_received_msg_s': avg_received,
                'worst_p95_ms': worst_p95,
                'producers': producers,
                'consumers': consumers,
                'size_bytes': size_bytes,
                
                # 归一化指标（单位核心）
                'msg_per_sec_per_core': round(msg_per_sec_per_core, 2),
                
                # 归一化指标（单位资源）
                'msg_per_sec_per_producer': round(msg_per_sec_per_producer, 2),
                'msg_per_sec_per_consumer': round(msg_per_sec_per_consumer, 2),
                'msg_per_sec_per_gb_memory': round(msg_per_sec_per_gb, 2),
                'msg_per_sec_per_kb': round(msg_per_sec_per_kb, 2),
                
                # 单位消息开销
                'latency_per_msg_ms': round(latency_per_msg, 2),
                'memory_per_msg_bytes': round(estimated_mem_per_msg, 2),
                
                # 吞吐指标
                'throughput_mbps': round(throughput_mbps, 2),
                
                # 资源利用率
                'cpu_utilization_pct': round(cpu_utilization, 2),
                'loss_ratio': round(loss_ratio, 4),
                
                # 测试环境
                'test_cpu_cores': self.cpu_cores,
                'test_memory_gb': self.memory_gb,
            })
        
        return pd.DataFrame(results)
    
    def generate_capacity_extrapolation(self, normalized_df: pd.DataFrame, target_slo: Dict) -> pd.DataFrame:
        """
        基于SLO反推所需资源
        
        Args:
            normalized_df: 归一化后的指标DataFrame
            target_slo: 目标SLO约束，例如：
                {
                    'component_type': 'DB',
                    'target_tps': 10000,
                    'max_latency_ms': 50,
                    'component_type': 'MQ',
                    'target_msg_per_sec': 50000,
                    'max_p95_ms': 100
                }
        
        Returns:
            资源配置建议DataFrame
        """
        recommendations = []
        
        if target_slo.get('component_type') == 'DB':
            # 基于TPS需求反推
            target_tps = target_slo.get('target_tps', 0)
            max_latency = target_slo.get('max_latency_ms', 1000)
            
            # 找到满足延迟要求的基准数据
            valid_data = normalized_df[
                (normalized_df['component_type'] == 'DB') &
                (normalized_df['latency_ms'] <= max_latency)
            ]
            
            if len(valid_data) > 0:
                # 使用最佳性能数据（最高TPS/核心）
                best = valid_data.loc[valid_data['tps_per_core'].idxmax()]
                
                # 计算所需核心数
                required_cores = int(np.ceil(target_tps / best['tps_per_core']))
                
                # 计算所需内存（基于tps_per_gb）
                required_memory_gb = int(np.ceil(target_tps / best['tps_per_gb_memory']))
                
                # 估算实际延迟（假设线性扩展）
                estimated_latency = best['latency_ms'] * (target_tps / best['tps'])
                
                recommendations.append({
                    'component': best['component'],
                    'target_tps': target_tps,
                    'max_latency_ms': max_latency,
                    'required_cpu_cores': required_cores,
                    'required_memory_gb': required_memory_gb,
                    'estimated_latency_ms': round(estimated_latency, 2),
                    'baseline_tps_per_core': best['tps_per_core'],
                    'baseline_tps_per_gb': best['tps_per_gb_memory'],
                    'baseline_test_tps': best['tps'],
                    'baseline_test_latency_ms': best['latency_ms'],
                })
        
        elif target_slo.get('component_type') == 'MQ':
            # 基于消息吞吐需求反推
            target_msg_per_sec = target_slo.get('target_msg_per_sec', 0)
            max_p95 = target_slo.get('max_p95_ms', 2000)
            
            # 找到满足延迟要求的基准数据
            valid_data = normalized_df[
                (normalized_df['component_type'] == 'MQ') &
                (normalized_df['worst_p95_ms'] <= max_p95)
            ]
            
            if len(valid_data) > 0:
                # 使用最佳性能数据
                best = valid_data.loc[valid_data['msg_per_sec_per_core'].idxmax()]
                
                # 计算所需核心数
                required_cores = int(np.ceil(target_msg_per_sec / best['msg_per_sec_per_core']))
                
                # 计算所需内存
                required_memory_gb = int(np.ceil(target_msg_per_sec / best['msg_per_sec_per_gb_memory']))
                
                # 估算实际延迟
                estimated_p95 = best['worst_p95_ms'] * (target_msg_per_sec / best['avg_received_msg_s'])
                
                recommendations.append({
                    'component': best['component'],
                    'target_msg_per_sec': target_msg_per_sec,
                    'max_p95_ms': max_p95,
                    'required_cpu_cores': required_cores,
                    'required_memory_gb': required_memory_gb,
                    'estimated_p95_ms': round(estimated_p95, 2),
                    'baseline_msg_per_sec_per_core': best['msg_per_sec_per_core'],
                    'baseline_msg_per_sec_per_gb': best['msg_per_sec_per_gb_memory'],
                    'baseline_test_msg_per_sec': best['avg_received_msg_s'],
                    'baseline_test_p95_ms': best['worst_p95_ms'],
                })
        
        return pd.DataFrame(recommendations)


def main():
    parser = argparse.ArgumentParser(
        description="归一化建模：将测试结果转换为可外推的单位指标"
    )
    parser.add_argument(
        '--db-csv',
        type=str,
        help='数据库测试结果CSV文件路径'
    )
    parser.add_argument(
        '--mq-summary-csv',
        type=str,
        help='消息队列测试汇总CSV文件路径'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='datas',
        help='输出目录（默认：datas）'
    )
    parser.add_argument(
        '--cpu-cores',
        type=int,
        default=4,
        help='测试环境CPU核心数（默认：4）'
    )
    parser.add_argument(
        '--memory-gb',
        type=float,
        default=4.0,
        help='测试环境内存大小GB（默认：4.0）'
    )
    parser.add_argument(
        '--component-name-db',
        type=str,
        default='KingbaseES',
        help='数据库组件名称（默认：KingbaseES）'
    )
    parser.add_argument(
        '--component-name-mq',
        type=str,
        default='RabbitMQ',
        help='消息队列组件名称（默认：RabbitMQ）'
    )
    
    args = parser.parse_args()
    
    # 创建归一化计算器
    normalizer = NormalizedMetrics(
        cpu_cores=args.cpu_cores,
        memory_gb=args.memory_gb
    )
    
    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_normalized = []
    
    # 处理数据库数据
    if args.db_csv:
        db_path = pathlib.Path(args.db_csv)
        if db_path.exists():
            print(f"处理数据库测试结果: {db_path}")
            db_df = pd.read_csv(db_path)
            db_normalized = normalizer.normalize_db_metrics(db_df, args.component_name_db)
            all_normalized.append(db_normalized)
            
            output_file = output_dir / f"normalized_db_{args.component_name_db}_{timestamp}.csv"
            db_normalized.to_csv(output_file, index=False, encoding='utf-8')
            print(f"  已保存归一化指标: {output_file}")
            print(f"  共 {len(db_normalized)} 条记录")
        else:
            print(f"警告: 数据库CSV文件不存在: {db_path}")
    
    # 处理消息队列数据
    if args.mq_summary_csv:
        mq_path = pathlib.Path(args.mq_summary_csv)
        if mq_path.exists():
            print(f"处理消息队列测试结果: {mq_path}")
            mq_df = pd.read_csv(mq_path)
            mq_normalized = normalizer.normalize_mq_metrics(mq_df, args.component_name_mq)
            all_normalized.append(mq_normalized)
            
            output_file = output_dir / f"normalized_mq_{args.component_name_mq}_{timestamp}.csv"
            mq_normalized.to_csv(output_file, index=False, encoding='utf-8')
            print(f"  已保存归一化指标: {output_file}")
            print(f"  共 {len(mq_normalized)} 条记录")
        else:
            print(f"警告: 消息队列CSV文件不存在: {mq_path}")
    
    # 合并所有归一化结果
    if all_normalized:
        combined = pd.concat(all_normalized, ignore_index=True)
        combined_file = output_dir / f"normalized_all_{timestamp}.csv"
        combined.to_csv(combined_file, index=False, encoding='utf-8')
        print(f"\n合并归一化指标已保存: {combined_file}")
        print(f"总计 {len(combined)} 条记录")
        
        # 打印统计摘要
        print("\n=== 归一化指标统计摘要 ===")
        for comp_type in combined['component_type'].unique():
            comp_data = combined[combined['component_type'] == comp_type]
            print(f"\n{comp_type} 组件 ({comp_data['component'].iloc[0]}):")
            
            if comp_type == 'DB':
                print(f"  TPS/核心: 平均 {comp_data['tps_per_core'].mean():.2f}, "
                      f"最大 {comp_data['tps_per_core'].max():.2f}")
                print(f"  TPS/GB内存: 平均 {comp_data['tps_per_gb_memory'].mean():.2f}, "
                      f"最大 {comp_data['tps_per_gb_memory'].max():.2f}")
                print(f"  延迟/事务: 平均 {comp_data['latency_per_tx_ms'].mean():.2f} ms")
            
            elif comp_type == 'MQ':
                print(f"  消息/秒/核心: 平均 {comp_data['msg_per_sec_per_core'].mean():.2f}, "
                      f"最大 {comp_data['msg_per_sec_per_core'].max():.2f}")
                print(f"  消息/秒/GB内存: 平均 {comp_data['msg_per_sec_per_gb_memory'].mean():.2f}, "
                      f"最大 {comp_data['msg_per_sec_per_gb_memory'].max():.2f}")
                print(f"  P95延迟: 平均 {comp_data['worst_p95_ms'].mean():.2f} ms")
    
    else:
        print("错误: 没有找到有效的测试数据文件")


if __name__ == "__main__":
    main()

