from minio import Minio
from io import BytesIO
from minio.commonconfig import REPLACE, CopySource
import random
import time
from datetime import datetime
import uuid

def setup_minio_client():
    """初始化並返回 MinIO 客戶端"""
    return Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

def ensure_bucket_exists(client, bucket_name):
    """確保指定的儲存桶存在，如果不存在就創建"""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"已建立儲存桶: {bucket_name}")

def upload_file(client, bucket_name, object_name, data):
    """上傳檔案到 MinIO"""
    try:
        if isinstance(data, bytes):
            data_stream = BytesIO(data)
            data_length = len(data)
        else:
            data_stream = data
            data.seek(0, 2)
            data_length = data.tell()
            data.seek(0)
        
        client.put_object(
            bucket_name,
            object_name,
            data=data_stream,
            length=data_length
        )
        print(f"成功上傳 {object_name} 到 {bucket_name}")
        return True
    except Exception as e:
        print(f"檔案上傳錯誤: {e}")
        return False

def update_object_metadata(client, bucket_name, object_name, metadata):
    """更新物件的 metadata"""
    try:
        copy_source = CopySource(bucket_name, object_name)
        result = client.copy_object(
            bucket_name,
            object_name,
            copy_source,
            metadata=metadata,
            metadata_directive=REPLACE
        )
        print(f"成功更新 {object_name} 的 metadata")
        return True
    except Exception as e:
        print(f"更新 metadata 錯誤: {e}")
        return False

def generate_test_data():
    """生成測試的 API metadata"""
    api_templates = [
        "https://coffee.alexflipnote.dev/{}_coffee.jpg",
        "https://dog.ceo/api/breeds/{}/images/random",
        "https://api.thecatapi.com/v1/images/{}",
        "https://picsum.photos/id/{}/info"
    ]
    
    test_id = str(random.randint(1000, 9999))
    api_template = random.choice(api_templates)
    api_url = api_template.format(test_id)
    
    return {
        "X-Amz-Meta-Api": api_url,  # 使用正確的 metadata key
        "X-Amz-Meta-Id": test_id    # 使用正確的 metadata key
    }

def generate_mlflow_path():
    """生成 MLflow 格式的路徑"""
    # 生成隨機的 run_id (使用 uuid 的前32位)
    run_id = uuid.uuid4().hex
    
    # 生成三種不同類型的文件名
    file_types = [
        "model.pkl",
        "conda.yaml",
        "requirements.txt"
    ]
    file_name = random.choice(file_types)
    
    # 構建完整的路徑
    if file_name == "model.pkl":
        path = f"if_mlflow.mimir/{run_id}/artifacts/model/{file_name}"
    else:
        path = f"if_mlflow.mimir/{run_id}/artifacts/{file_name}"
    
    return path

def main(num_objects=5):
    client = setup_minio_client()
    bucket_name = "test-bucket"
    ensure_bucket_exists(client, bucket_name)
    
    uploaded_objects = []
    
    for i in range(num_objects):
        # 生成 MLflow 格式的物件名稱
        object_name = generate_mlflow_path()
        
        # 生成測試數據
        test_data = f"MLflow test data {i+1} - {datetime.now()}".encode('utf-8')
        
        # 上傳檔案
        if upload_file(client, bucket_name, object_name, test_data):
            print(f"檔案 {i+1} 上傳完成")
            
            # 生成並更新 metadata
            metadata = generate_test_data()
            
            if update_object_metadata(client, bucket_name, object_name, metadata):
                print(f"Metadata {i+1} 更新完成")
                uploaded_objects.append({
                    "object_name": object_name,
                    "metadata": metadata
                })
        
        # 添加延遲避免過快上傳
        time.sleep(0.5)
    
    # 打印上傳結果摘要
    print("\n上傳摘要:")
    for obj in uploaded_objects:
        print(f"\n完整路徑: {obj['object_name']}")
        print(f"API URL: {obj['metadata']['X-Amz-Meta-Api']}")
        print(f"ID: {obj['metadata']['X-Amz-Meta-Id']}")
        
        # 解析並打印路徑組件
        parts = obj['object_name'].split('/')
        print(f"Bucket Name: {parts[0]}")
        print(f"Path Name: {'/'.join(parts[1:-1])}")
        print(f"Model Name: {parts[-1]}")

if __name__ == "__main__":
    main(num_objects=5)