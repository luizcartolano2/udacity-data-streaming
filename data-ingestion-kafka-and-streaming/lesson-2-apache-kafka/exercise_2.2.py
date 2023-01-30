import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = "PLAINTEXT://localhost:29092"
# TOPIC_NAME = "com.udacity.lesson3.exercise2.clicks"
TOPIC_NAME = "test"

def topic_exists(client, topic_name):
    """Checks if the given topic exists"""
    topic_metadata = client.list_topics(timeout=5)
    return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))


def create_topic(client, topic_name):
    """Creates the topic with the given topic name"""
    futures = client.create_topics(
        [
            NewTopic(
                topic=topic_name,
                num_partitions=1,
                replication_factor=1,
                config={
                    "cleanup.policy": "delete",
                    "compression.type": "lz4",
                    "delete.retention.ms": "2000",
                    "file.delete.delay.ms": "2000",
                },
            )
        ]
    )
    for topic, future in futures.items():
        try:
            future.result()
            print("topic created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    topic_name = "test"
    exists = topic_exists(client, topic_name)
    print(f"Topic {topic_name} exists: {exists}")

    if exists is False:
        create_topic(client, topic_name)

    try:
        asyncio.run(produce_consume(topic_name))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:
        p.produce(topic_name, f"iteration {curr_iteration}".encode("utf-8"))
        curr_iteration += 1
        await asyncio.sleep(0.5)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(2.5)

def check_connection():
    # Example using confuent_kafka

    kafka_broker = {'bootstrap.servers': 'localhost:9092'}
    admin_client = AdminClient(kafka_broker)
    topics = admin_client.list_topics().topics
    print(topics)
    if not topics:
        raise RuntimeError()
1
2
3
def solution(array, commands):
    return list(map(lambda x:sorted(array[x[0]-1:x[1]])[x[2]-1], commands))

def solution(array, commands):
    answer = []
    for i in range(len(commands)): # i = 0, 1, 2
        left = commands[i][0] - 1
        right = commands[i][1] - 1
        ind = commands[i][2] -1
        sub_array = array[left:right]
        sub_array.sort()
        num = sub_array[ind]
        print(num)
        #answer.append(num)
#    return answer


if __name__ == "__main__":
    array = [1, 5, 2, 6, 3, 7, 4]
    commands = [[2, 5, 3], [4, 4, 1], [1, 7, 3]]
    solution(array, commands)
    # main()
    # check_connection()
    # client = AdminClient({"bootstrap.servers": BROKER_URL})
    # topic_exists(client, topic_name=TOPIC_NAME)
