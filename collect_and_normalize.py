#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据收集与归一化处理脚本
自动扫描测试结果目录，批量处理并生成归一化指标
"""

import pandas as pd
import pathlib
import argparse
from datetime import datetime
from typing import Optional
from normalize_metrics import NormalizedMetrics


def find_latest_csv(directory: pathlib.Path, pattern: str) -> Optional[pathlib.Path]:
    """查找最新的匹配CSV文件"""
    files = list(directory.glob(pattern))
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def batch_process(
    data_dir: str = "datas",
    cpu_cores: int = 4,
    memory_gb: float = 4.0,
    output_dir: str = "datas",
):
    """
    批量处理测试结果并生成归一化指标
    
    Args:
        data_dir: 测试结果数据目录
        cpu_cores: 测试环境CPU核心数
        memory_gb: 测试环境内存大小GB
        output_dir: 输出目录
    """
    data_path = pathlib.Path(data_dir)
    output_path = pathlib.Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    normalizer = NormalizedMetrics(cpu_cores=cpu_cores, memory_gb=memory_gb)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_normalized = []
    
    print("=== 开始批量处理测试结果 ===\n")
    
    # 查找数据库测试结果
    # 优先查找 results.csv；否则匹配常见命名：*_kbbench_results_*.csv 或包含 kbbench 的文件
    db_csv = data_path / "results.csv"
    if not db_csv.exists():
        # Kingbase 标准命名：<Component>_kbbench_results_*.csv（例如：KingbaseES_kbbench_results_2025...）
        db_csv = find_latest_csv(data_path, "*_kbbench_results_*.csv")
    if db_csv is None:
        # 兼容早期或自定义命名：包含 kbbench 的 CSV
        db_csv = find_latest_csv(data_path, "*kbbench*.csv")
    
    if db_csv and db_csv.exists():
        print(f"处理数据库测试结果: {db_csv}")
        try:
            db_df = pd.read_csv(db_csv)
            # 从文件名猜测组件名（若包含前缀），否则使用 KingbaseES
            comp_name = "KingbaseES"
            name = db_csv.name
            # 约定：{Component}_kbbench_results_*.csv 或 results.csv
            if "_kbbench_results_" in name:
                comp_name = name.split("_kbbench_results_")[0]
            db_normalized = normalizer.normalize_db_metrics(db_df, comp_name)
            
            if len(db_normalized) > 0:
                all_normalized.append(db_normalized)
                output_file = output_path / f"normalized_db_{comp_name}_{timestamp}.csv"
                db_normalized.to_csv(output_file, index=False, encoding='utf-8')
                print(f"  ✓ 已保存: {output_file}")
                print(f"  ✓ 记录数: {len(db_normalized)}")
                
                # 显示最佳性能数据
                best = db_normalized.loc[db_normalized['tps_per_core'].idxmax()]
                print(f"  ✓ 最佳TPS/核心: {best['tps_per_core']:.2f} (TPS={best['tps']:.2f}, 并发={best['clients']})")
            else:
                print(f"  ⚠ 未找到有效数据")
        except Exception as e:
            print(f"  ✗ 处理失败: {e}")
    else:
        print("⚠ 未找到数据库测试结果文件")
    
    print()
    
    # 查找消息队列测试结果（允许组件名前缀）
    # 标准命名：<Component>_perftest_summary_*.csv（例如：RabbitMQ_perftest_summary_2025...）
    mq_csv = find_latest_csv(data_path, "*perftest_summary_*.csv")
    if mq_csv and mq_csv.exists():
        print(f"处理消息队列测试结果: {mq_csv}")
        try:
            mq_df = pd.read_csv(mq_csv)
            # 从 perftest_summary 推断组件名：{Component}_perftest_summary_*.csv
            comp_name = "RabbitMQ"
            name = mq_csv.name
            if name.endswith('.csv') and "_perftest_summary_" in name:
                comp_name = name.split("_perftest_summary_")[0]
            mq_normalized = normalizer.normalize_mq_metrics(mq_df, comp_name)
            
            if len(mq_normalized) > 0:
                all_normalized.append(mq_normalized)
                output_file = output_path / f"normalized_mq_{comp_name}_{timestamp}.csv"
                mq_normalized.to_csv(output_file, index=False, encoding='utf-8')
                print(f"  ✓ 已保存: {output_file}")
                print(f"  ✓ 记录数: {len(mq_normalized)}")
                
                # 显示最佳性能数据
                best = mq_normalized.loc[mq_normalized['msg_per_sec_per_core'].idxmax()]
                print(f"  ✓ 最佳消息/秒/核心: {best['msg_per_sec_per_core']:.2f} "
                      f"(消息/秒={best['avg_received_msg_s']:.2f})")
            else:
                print(f"  ⚠ 未找到有效数据")
        except Exception as e:
            print(f"  ✗ 处理失败: {e}")
    else:
        print("⚠ 未找到消息队列测试结果文件")
    
    # 合并所有归一化结果
    if all_normalized:
        print("\n=== 生成合并归一化指标 ===")
        combined = pd.concat(all_normalized, ignore_index=True)
        combined_file = output_path / f"normalized_all_{timestamp}.csv"
        combined.to_csv(combined_file, index=False, encoding='utf-8')
        print(f"✓ 合并文件: {combined_file}")
        print(f"✓ 总计记录: {len(combined)}")
        
        # 生成统计摘要
        print("\n=== 归一化指标统计摘要 ===")
        for comp_type in combined['component_type'].unique():
            comp_data = combined[combined['component_type'] == comp_type]
            comp_name = comp_data['component'].iloc[0]
            
            print(f"\n【{comp_name} ({comp_type})】")
            print("-" * 50)
            
            if comp_type == 'DB':
                print(f"单位核心吞吐 (TPS/核心):")
                print(f"  平均: {comp_data['tps_per_core'].mean():.2f}")
                print(f"  最大: {comp_data['tps_per_core'].max():.2f}")
                print(f"  最小: {comp_data['tps_per_core'].min():.2f}")
                print(f"  中位数: {comp_data['tps_per_core'].median():.2f}")
                
                print(f"\n单位内存吞吐 (TPS/GB):")
                print(f"  平均: {comp_data['tps_per_gb_memory'].mean():.2f}")
                print(f"  最大: {comp_data['tps_per_gb_memory'].max():.2f}")
                
                print(f"\n单位事务延迟:")
                print(f"  平均: {comp_data['latency_per_tx_ms'].mean():.2f} ms")
                print(f"  最小: {comp_data['latency_per_tx_ms'].min():.2f} ms")
                
                print(f"\n资源利用率估算:")
                print(f"  CPU利用率: 平均 {comp_data['cpu_utilization_pct'].mean():.2f}%")
            
            elif comp_type == 'MQ':
                print(f"单位核心吞吐 (消息/秒/核心):")
                print(f"  平均: {comp_data['msg_per_sec_per_core'].mean():.2f}")
                print(f"  最大: {comp_data['msg_per_sec_per_core'].max():.2f}")
                print(f"  最小: {comp_data['msg_per_sec_per_core'].min():.2f}")
                
                print(f"\n单位内存吞吐 (消息/秒/GB):")
                print(f"  平均: {comp_data['msg_per_sec_per_gb_memory'].mean():.2f}")
                print(f"  最大: {comp_data['msg_per_sec_per_gb_memory'].max():.2f}")
                
                print(f"\n延迟指标:")
                print(f"  P95延迟: 平均 {comp_data['worst_p95_ms'].mean():.2f} ms")
                print(f"  P95延迟: 最小 {comp_data['worst_p95_ms'].min():.2f} ms")
                
                print(f"\n吞吐带宽:")
                print(f"  平均: {comp_data['throughput_mbps'].mean():.2f} MB/s")
                print(f"  最大: {comp_data['throughput_mbps'].max():.2f} MB/s")
                
                print(f"\n消息丢失率:")
                print(f"  平均: {comp_data['loss_ratio'].mean():.4f}")
        
        print("\n" + "=" * 60)
        print("归一化建模完成！指标已保存，可用于容量外推计算。")
    else:
        print("\n⚠ 未找到任何有效数据，请检查数据文件路径")


def capacity_extrapolation_example(
    normalized_file: str,
    target_slo: dict,
    output_dir: str = "datas",
):
    """
    容量外推示例：基于SLO反推所需资源
    
    Args:
        normalized_file: 归一化指标文件路径
        target_slo: 目标SLO约束
        output_dir: 输出目录
    """
    print("=== 容量外推计算 ===")
    
    df = pd.read_csv(normalized_file)
    normalizer = NormalizedMetrics()
    
    recommendations = normalizer.generate_capacity_extrapolation(df, target_slo)
    
    if len(recommendations) > 0:
        output_path = pathlib.Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_path / f"capacity_recommendation_{timestamp}.csv"
        recommendations.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"\n✓ 资源配置建议已保存: {output_file}\n")
        print(recommendations.to_string(index=False))
    else:
        print("⚠ 未找到满足SLO要求的基准数据")


def main():
    parser = argparse.ArgumentParser(
        description="数据收集与归一化处理工具"
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default='datas',
        help='测试结果数据目录（默认：datas）'
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
        '--extrapolate',
        action='store_true',
        help='执行容量外推示例（需要先运行归一化处理）'
    )
    parser.add_argument(
        '--target-tps',
        type=int,
        help='目标TPS（用于容量外推）'
    )
    parser.add_argument(
        '--target-msg-per-sec',
        type=int,
        help='目标消息/秒（用于容量外推）'
    )
    parser.add_argument(
        '--max-latency-ms',
        type=int,
        default=50,
        help='最大延迟ms（用于容量外推，默认：50）'
    )
    
    args = parser.parse_args()
    
    # 批量处理
    batch_process(
        data_dir=args.data_dir,
        cpu_cores=args.cpu_cores,
        memory_gb=args.memory_gb,
        output_dir=args.output_dir,
    )
    
    # 容量外推示例
    if args.extrapolate:
        normalized_file = pathlib.Path(args.output_dir) / "normalized_all_*.csv"
        files = list(pathlib.Path(args.output_dir).glob("normalized_all_*.csv"))
        if files:
            latest_file = max(files, key=lambda p: p.stat().st_mtime)
            
            if args.target_tps:
                target_slo = {
                    'component_type': 'DB',
                    'target_tps': args.target_tps,
                    'max_latency_ms': args.max_latency_ms,
                }
                capacity_extrapolation_example(
                    str(latest_file),
                    target_slo,
                    args.output_dir
                )
            
            if args.target_msg_per_sec:
                target_slo = {
                    'component_type': 'MQ',
                    'target_msg_per_sec': args.target_msg_per_sec,
                    'max_p95_ms': args.max_latency_ms,
                }
                capacity_extrapolation_example(
                    str(latest_file),
                    target_slo,
                    args.output_dir
                )


if __name__ == "__main__":
    main()

