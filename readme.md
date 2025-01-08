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


### 默認帳號
- Airflow: 
  - 用戶名: airflow
  - 密碼: airflow

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

## Dataset 概念解釋

### Dataset
- Dataset 是 Airflow 中代表資料集的邏輯概念
- 通過 URI 唯一標識，例如：`s3://bucket/path/data.csv`
- 可以被多個 DAG 產出或消費

### Producer & Consumer
1. Producer (生產者)
   - 產出或更新 dataset 的 DAG
   - 使用 `outlets` 參數來標記產出的 dataset

2. Consumer (消費者)
   - 依賴 dataset 的 DAG
   - 使用 `schedule=[dataset]` 來訂閱 dataset 更新

## API 規格

### Endpoint
```
POST /api/v1/datasets/events
```

### Request Body
```json
{
    "dataset_uri": "your-dataset-uri",
    "extra": {}  // 選填，可加入額外資訊
}
```

### Response
- 200: Success
- 400: Invalid arguments
- 401: Unauthorized
- 403: Permission denied
- 404: Resource not found

## 使用流程

1. 設定認證
```bash
# 在 .env 文件中設定
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
```

2. 定義 Consumer DAG
```python
from airflow import DAG
from airflow.datasets import Dataset

my_dataset = Dataset("s3://bucket/data.csv")

with DAG(
    'consumer_dag',
    schedule=[my_dataset],
    ...) as dag:
    # 你的任務定義
```

3. 觸發 Dataset Event
```python
import requests
import base64

def trigger_dataset_event():
    # 設定
    AIRFLOW_URL = "http://localhost:8080/api/v1"
    username = "admin"
    password = "admin"
    
    # Basic Auth
    credentials = base64.b64encode(
        f"{username}:{password}".encode()
    ).decode()
    
    # Headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {credentials}"
    }
    
    # Payload
    payload = {
        "dataset_uri": "s3://bucket/data.csv"
    }
    
    # 發送請求
    response = requests.post(
        f"{AIRFLOW_URL}/datasets/events",
        headers=headers,
        json=payload
    )
    
    return response
```

## 使用情境

1. 外部系統整合
   - 當外部系統完成資料處理時觸發 Airflow DAG

2. 事件驅動工作流
   - 基於資料更新自動觸發相關處理流程

3. 跨 DAG 依賴
   - 建立基於資料的 DAG 之間的依賴關係

4. 資料血緣追蹤
   - 追蹤資料的產出和消費關係

## 最佳實踐

1. URI 命名規範
   - 使用清晰且一致的命名方式
   - 包含必要的版本資訊

2. 錯誤處理
   - 實作適當的錯誤處理機制
   - 記錄 API 呼叫結果

3. 監控
   - 監控 Dataset Event 的觸發狀況
   - 設置適當的告警機制

4. 安全性
   - 妥善保管認證資訊
   - 實施適當的存取控制

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
