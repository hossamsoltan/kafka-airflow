from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

KAFKA_TOPIC = "airflow2"

def producer_function(**context):
    # This should yield key-value pairs
    yield 'key', 'message to send'

@dag(
    start_date=datetime(2024, 12, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def produce_consume_treats():

    produce_kafka = ProduceToTopicOperator(
        task_id="produce_kafka",
        kafka_config_id="kafka_default",
        topic=KAFKA_TOPIC,
        producer_function=producer_function
    )

    consume_kafka = ConsumeFromTopicOperator(
        task_id="consume_kafka",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC]
    )

    produce_kafka >> consume_kafka


produce_consume_treats()
