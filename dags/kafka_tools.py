"""
# kafka_tools
This module is for kafka user
"""
import os
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer
from confluent_kafka import Consumer
from confluent_kafka import Consumer, KafkaException, KafkaError

kafka_config = {
    # 'security.protocol': 'PLAINTEXT',
    # 'sasl.mechanism': 'SCRAM-SHA-512'
}

def check_type_is_dict(config_dict):
    """
    check type
    """
    if isinstance(config_dict, dict):
        pass
    else:
        err_msg = "type of input variable must be dict, " + \
            f"not {type(config_dict)}"
        raise TypeError(err_msg)

def set_conn(config_dict):
    """
    set kafka ip
    """
    if "bootstrap.servers" in config_dict:
        pass
    else:
        config_dict["bootstrap.servers"] = os.environ["KAFKA_IP"]
    return config_dict

def get_kafka_config(config_dict):
    """
    parse config
    """
    check_type_is_dict(config_dict)
    config_dict = set_conn(config_dict)
    for key, value in config_dict.items():
        kafka_config[key] = value
    return kafka_config

class AdminTool(AdminClient):
    """
    kafka admin module
    """
    def __init__(self, config_dict=None):
        _kafka_config = get_kafka_config(config_dict)
        super().__init__(_kafka_config)

class ProducerTool(Producer):
    """
    kafka produce module
    """
    def __init__(self, config_dict=None):
        _kafka_config = get_kafka_config(config_dict)
        super().__init__(_kafka_config)

class ConsumerTool(Consumer):
    """
    kafka consumer module
    """
    def __init__(self, config_dict=None):
        _kafka_config = get_kafka_config(config_dict)
        super().__init__(_kafka_config)

    def subscribe(
        self, subscribed_topic_list
    ):
        """
        subscribe topics
        Arg:
            - subscribed_topic_list
        Return:
            - msg
        """
        if type(subscribed_topic_list) is list:
            exist_topic_list = self.list_topics().topics
        else:
            raise TypeError(
                "type of subscribed_topic_list must be list, "
                + f"not {type(subscribed_topic_list)}"
            )

        not_exist_topic_list = list(
            set(subscribed_topic_list) - set(exist_topic_list)
        )

        if len(not_exist_topic_list) > 0:
            err_msg = {
                "msg": "Subscribed topic doesn't exist",
                "failed_topic": not_exist_topic_list,
                "success": False
            }
            raise ValueError(err_msg)
        elif len(subscribed_topic_list) == 0:
            err_msg = {
                "msg": "Subscribed topic list is null",
                "success": False
            }
            raise ValueError(err_msg)
        else:
            super().subscribe(subscribed_topic_list)
            msg = {
                "msg": "Subscribed topic list success",
                "topic": subscribed_topic_list,
                "success": True
            }
            return msg