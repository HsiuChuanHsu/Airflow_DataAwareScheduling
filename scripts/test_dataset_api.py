import requests
import json
import base64

def test_dataset_event():
    # Airflow webserver 的位置
    AIRFLOW_URL = "http://localhost:8080/api/v1"
    
    # Basic Auth credentials
    username = "admin"
    password = "admin"
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    
    
    # API endpoint
    endpoint = f"{AIRFLOW_URL}/datasets/events"
    
    # Request headers with Basic Auth
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {credentials}"
    }
    
    # Request payload
    db_name = 'test'
    schema_name = 'test'
    payload = {
        "dataset_uri": f"file:///opt/airflow/files/dataset4_data.json",
        "extra": {}
    }
    
    try:
        # 發送 POST 請求
        response = requests.post(
            endpoint,
            headers=headers,
            json=payload
        )
        
        # 輸出結果
        print(f"Status Code: {response.status_code}")
        print("Response:")
        print(json.dumps(response.json(), indent=2) if response.content else "No content")
        
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    test_dataset_event()