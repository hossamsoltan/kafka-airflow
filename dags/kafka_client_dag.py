from airflow.decorators import dag, task
from pendulum import datetime
from confluent_kafka import Producer, Consumer

KAFKA_TOPIC = "airflow2"

@dag(
    start_date=datetime(2024, 12, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def python_kafka_client():

    @task
    def produce():
        conf = {'bootstrap.servers': 'broker:29092'}
        producer = Producer(conf)

        def delivery_report(err, msg):
            if err:
                print(f"Message delivery failed: {err}")
            else:
                print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

        producer.produce(KAFKA_TOPIC, key="key", value="Python client message", callback=delivery_report)
        producer.flush()

    @task
    def consume():
        conf = {'bootstrap.servers': 'broker:29092', 'group.id': 'airflow-group', 'auto.offset.reset': 'earliest'}
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC])

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                break
            print(f"Received message: {msg.value().decode('utf-8')}")

        consumer.close()

    produce() >> consume()


python_kafka_client()
