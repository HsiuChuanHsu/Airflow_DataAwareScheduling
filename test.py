def parse_mlflow_path(key: str):
    """
    解析 MLflow 路徑
    例如: "if_mlflow.mimir/fbb8f072ea1c4191a846da7474684dd5/artifacts/model/model.pkl"
    返回: (bucket_name, path_name, model_name)
    """
    try:
        # 分割路徑
        parts = key.split('/')
        
        # 獲取 bucket_name (例如: if_mlflow.mimir)
        bucket_name = parts[0]
        
        # 獲取最後的文件名作為 model_name (例如: model.pkl)
        model_name = parts[-1]
        
        # 中間的部分作為 path_name
        path_name = '/'.join(parts[1:-1])
        
        return bucket_name, path_name, model_name
    except Exception as e:
        logging.error(f"Error parsing MLflow path: {e}")
        return None, None, None

class BatchKafkaOperator(BaseOperator):
    def __init__(
        self, 
        batch_size: int = 10,
        timeout: float = 30.0,
        topics: List[str] = None,
        bootstrap_servers: str = 'broker:29092',
        group_id: str = 'airflow_consumer_group_batch',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.batch_size = batch_size
        self.timeout = timeout
        self.topics = topics or ['test-topic_batch']
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

    def execute(self, context):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'security.protocol': 'PLAINTEXT',
        }

        consumer = ConsumerTool(conf)
        subscribe_response = consumer.subscribe(self.topics)
        
        if not subscribe_response['success']:
            logging.error(f"Failed to subscribe to topics {self.topics}: {subscribe_response['msg']}")
            return None

        try:
            processed_count = 0
            messages_processed = []

            while processed_count < self.batch_size:
                msg = consumer.poll(timeout=self.timeout)
                
                if msg is None:
                    logging.info("No more messages to process")
                    break
                    
                if msg.error():
                    error_code = msg.error().code()
                    if error_code == KafkaError._PARTITION_EOF:
                        logging.info("Reached end of partition")
                    else:
                        logging.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Parse message value
                    message_data = json.loads(msg.value().decode('utf-8'))
                    
                    # 只處理 ObjectCreated:Copy 事件
                    if message_data.get('EventName') == 's3:ObjectCreated:Copy':
                        records = message_data.get('Records', [])
                        for record in records:
                            user_metadata = record.get('s3', {}).get('object', {}).get('userMetadata', {})
                            api_url = user_metadata.get('X-Amz-Meta-Api')
                            api_id = user_metadata.get('X-Amz-Meta-Id')
                            
                            # 獲取並解析 MLflow 路徑
                            key = record.get('s3', {}).get('object', {}).get('key', '')
                            bucket_name, path_name, model_name = parse_mlflow_path(key)
                            
                            if api_url and api_id and all([bucket_name, path_name, model_name]):
                                messages_processed.append({
                                    'api_url': api_url,
                                    'api_id': api_id,
                                    'event_time': record.get('eventTime'),
                                    'bucket_name': bucket_name,
                                    'path_name': path_name,
                                    'model_name': model_name,
                                    'partition': msg.partition(),
                                    'offset': msg.offset(),
                                    'raw_data': message_data
                                })
                                processed_count += 1
                                logging.info(f"Found valid message: API URL={api_url}, ID={api_id}, Path={key}")
                    
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding message: {e}")
                    continue
                except Exception as e:
                    logging.error(f"Error processing message: {e}")
                    continue

            if not messages_processed:
                raise AirflowSkipException("No valid messages were processed")

            logging.info(f"Batch processing completed. Found {len(messages_processed)} valid messages.")
            return messages_processed
        
        finally:
            consumer.close()