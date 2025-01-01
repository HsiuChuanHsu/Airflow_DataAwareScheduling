# Airflow Kafka Integration Project

這個專案實現了 Apache Airflow 與 Kafka 的整合，用於數據流處理和動態 DAG 觸發。專案使用 Docker Compose 來管理所有服務，包括 Airflow、Kafka、Zookeeper、Schema Registry 等組件。

## 系統架構

### 主要組件
- Apache Airflow (調度器和 Web 服務器)
- Apache Kafka (消息佇列)
- PostgreSQL (數據存儲)
- Confluent Schema Registry
- Klaw (Kafka 管理介面)
- Zookeeper (Kafka 協調服務)

### 功能特點
- 批量消息處理
- 動態 Dataset 觸發
- 消息歷史記錄追踪
- 自動化數據流程
- 可擴展的消息處理架構

## 快速開始

### 前置要求
- Docker
- Docker Compose
- Python 3.8+

### 安裝步驟

1. 複製專案
```bash
git clone <repository-url>
cd <project-directory>
```

2. 設置環境變數
```bash
cp .env.example .env
# 編輯 .env 文件設置必要的環境變數
```

3. 啟動服務
```bash
docker-compose up -d
```

4. 訪問服務
- Airflow UI: http://localhost:8080
- Kafka Control Center: http://localhost:9021
- Klaw: http://localhost:9097

### 默認帳號
- Airflow: 
  - 用戶名: airflow
  - 密碼: airflow
- Klaw:
  - 用戶名: admin
  - 密碼: admin

## 主要組件說明

### DAG 文件
1. `consumer_multi_with_process_dags.py`
   - Kafka 消費者 DAG
   - 實現批量消息處理
   - 支援動態 Dataset 觸發

2. `consumer_multi_with_process_down_dags.py`
   - 下游處理 DAG
   - 基於 Dataset 觸發
   - 實現數據處理邏輯

### 自定義操作器
- `BatchDynamicOutletKafkaOperator`: 批量處理 Kafka 消息
- `DatasetOutletOperator`: 設置動態 Dataset 觸發
- `LoadOperator`: 數據加載和歷史記錄

### Kafka 生產者
`kafka_producer.py` 提供測試數據生成功能：
- 支援多種數據類型
- 可配置消息發送頻率
- 自動生成測試數據

## 配置說明

### Kafka 配置
```python
KAFKA_TOPICS = {
    'batch_topics': ['test-topic_batch'],
    'bootstrap_servers': 'broker:29092',
    'group_id': 'airflow_consumer_group_batch'
}
```

### 數據庫配置
PostgreSQL 連接配置在 Airflow 的連接設置中：
- Connection ID: postgres_default
- 默認數據庫: airflow
- 默認 Schema: public

## 開發指南

### 添加新的數據類型
1. 在 `DATASET_CONFIGS` 中定義新的數據集
2. 在 Kafka 生產者中添加新的數據類型
3. 創建對應的下游處理 DAG

### 自定義消息處理
1. 繼承 `BaseOperator` 創建新的操作器
2. 實現自定義的 `execute` 方法
3. 在 DAG 中使用新的操作器

## 監控和維護

### 日誌查看
- Airflow 日誌：`./airflow-logs` 目錄
- Kafka 日誌：通過 Control Center 查看
- 容器日誌：`docker-compose logs <service-name>`

### 健康檢查
所有服務都配置了健康檢查：
- 檢查間隔：30秒
- 超時時間：10秒
- 重試次數：5次
