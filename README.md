## 信创组件适配 · 性能数据收集与归一化建模

本项目用于在信创生态中对数据库（DB）与消息队列（MQ）进行性能采集与归一化建模，产出可外推的单位指标，并可基于 SLO 进行容量外推。当前实现仅关注离线数据收集与分析，不包含在线后端 API。

### 功能概览
- 数据采集脚本：
  - `test_kingbase.py`：在 Docker 容器中运行 KingbaseES 的 `kbbench`，支持单次与并发扫描两种模式，解析并采集结果。
  - `test_rabbitmq.py`：调用 `perf-test.jar`（RabbitMQ 官方）进行速率探测，输出时序与汇总 CSV。
- 归一化建模：
  - `normalize_metrics.py`：将 DB/MQ 的测试结果转换为单位核心、单位内存、单位消息/事务等指标。
  - `collect_and_normalize.py`：自动扫描 `datas/` 下的结果文件，批量归一化并生成统计摘要；可选执行容量外推示例。

### 目录结构
- `test_kingbase.py`：运行 `kbbench` 并解析/保存结果，且支持 `-c/clients` 序列或范围扫描。
- `test_rabbitmq.py`：RabbitMQ 最大稳定吞吐自动探测（指数+二分），产出 CSV。
- `normalize_metrics.py`：归一化计算器与容量外推工具。
- `collect_and_normalize.py`：批处理入口（扫描→归一化→摘要→可选外推）。
- `datas/`：样例数据与归一化输出目录。

### 环境与依赖
- 基础环境：Linux（已在 Kylin V10 SP1 测试），Python 3.8+。
- Python 依赖：`pandas`、`numpy`
  ```bash
  pip install pandas==2.1.3 numpy==1.26.2
  ```
- RabbitMQ 测试依赖：
  - 需要 Java 运行环境（`java` 在 PATH）。
  - `perf-test.jar`（放在项目根目录或通过 `--jar` 指定；也可通过环境变量 `PERFTEST_JAR` 指定）。
  - RabbitMQ Broker 已可用，AMQP URI 可通过 `--uri` 或环境变量 `AMQP_URI` 提供。
- 数据库（KingbaseES）测试依赖：
  - Docker 已安装，且存在名为 `kingbase` 的容器，容器内可执行 `kbbench`。
  - 容器内可选存在 `sys_encpwd` 命令（自动生成免密文件），否则通过 `PGPASSWORD`/`KINGBASE_PASSWORD` 兜底。

提示：如不使用 Docker，也可仿照 `test_kingbase.py` 的 bash 片段在本机直接执行 `kbbench` 并重用解析函数。

### 数据格式（CSV）
- RabbitMQ 汇总（由 `test_rabbitmq.py` 生成，命名：`datas/<组件>_perftest_summary_*.csv`）：
  - 字段：`run_id,target_rate_msg_s,avg_sent_msg_s,avg_received_msg_s,worst_p95_ms,success,note,duration_s,producers,consumers,size_bytes,queue`
- RabbitMQ 时序（命名：`datas/<组件>_perftest_timeseries_*.csv`）：
  - 字段：`run_id,target_rate_msg_s,time_s,sent_msg_s,received_msg_s,p50_ms,p95_ms,p99_ms`
- KingbaseES 扫描结果（由 `test_kingbase.py` 生成，命名：`datas/<组件>_kbbench_results_*.csv` 或自定义）：
  - 字段：`timestamp,clients,jobs,duration_s,tps_including,tps_excluding,latency_ms_avg,tx_processed,return_code,error`

### 使用方法
#### 1) RabbitMQ 吞吐探测
```bash
python3 test_rabbitmq.py \
  --jar ./perf-test.jar \
  --uri amqp://guest:guest@127.0.0.1:5672/%2F \
  --producers 4 --consumers 4 --size 1024 --duration 15 \
  --start-rate 1000 --max-rate 20000 --growth 2.0 \
  --success-ratio 0.95 --p95-limit-ms 2000 \
  --out-dir datas --component-name RabbitMQ
```
输出：`datas/RabbitMQ_perftest_summary_*.csv` 与 `datas/RabbitMQ_perftest_timeseries_*.csv`

#### 2) KingbaseES 扫描
示例一：固定并发
```bash
python3 test_kingbase.py \
  --container kingbase --password 123456 --host 127.0.0.1 \
  --db kbbenchdb --user system --jobs 4 --duration 60 \
  --clients 16 --repeats 1 --component-name KingbaseES --print-output
```
输出：`datas/KingbaseES_kbbench_results_*.csv`

示例二：并发序列扫描（直接使用 `test_kingbase.py` 的扫描参数）
```bash
python3 test_kingbase.py \
  --container kingbase --password 123456 --host 127.0.0.1 \
  --db kbbenchdb --user system --jobs 4 --duration 60 \
  --clients-seq 350,380,400 \
  --repeats 2 --cooldown 2.0 \
  --out datas/KingbaseES_kbbench_results_$(date +%Y%m%d_%H%M%S).csv
```

#### 3) 归一化建模（单文件）
针对单次或单类结果进行归一化：
```bash
python3 normalize_metrics.py \
  --db-csv datas/KingbaseES_kbbench_results_20251013_202020.csv \
  --mq-summary-csv datas/RabbitMQ_perftest_summary_20251013_235231.csv \
  --cpu-cores 4 --memory-gb 4 \
  --output-dir datas
```
输出：
- `datas/normalized_db_<组件>_*.csv`
- `datas/normalized_mq_<组件>_*.csv`
- `datas/normalized_all_*.csv`

#### 4) 批处理与统计摘要（推荐）
自动扫描 `datas/` 下最新文件，生成归一化指标并打印摘要：
```bash
python3 collect_and_normalize.py \
  --data-dir datas \
  --output-dir datas \
  --cpu-cores 4 --memory-gb 4
```

可在同一命令后追加容量外推（先确保已生成 `normalized_all_*.csv`）：
```bash
# 以数据库目标TPS为例
python3 collect_and_normalize.py --extrapolate --target-tps 5000 --max-latency-ms 50

# 以消息队列目标消息/秒为例
python3 collect_and_normalize.py --extrapolate --target-msg-per-sec 20000 --max-latency-ms 2000
```

### 归一化建模方法
遵循“受限仿真 → 归一化建模 → 容量外推”的三段式：
- 归一化指标（核心）：
  - DB：`tps_per_core`、`tps_per_gb_memory`、`latency_per_tx_ms` 等。
  - MQ：`msg_per_sec_per_core`、`msg_per_sec_per_gb_memory`、`throughput_mbps`、`latency_per_msg_ms` 等。
- 单位内存开销：基于消息大小（MQ）或经验估算（DB）得到近似值（可替换为实际监控数据）。
- CPU 利用率（估算）：
  - DB：假设每核心 500 TPS 的上限进行归一化估计（`cpu_utilization_pct`）。
  - MQ：假设每核心 10000 msg/s 的上限进行归一化估计。



