from confluent_kafka import Consumer, SerializingProducer
from main import DATABASE_CONFIG, delivery_report
import psycopg2
import json
import random
from datetime import datetime

random.seed(42)

if __name__ == "__main__":
    consumer = None
    producer = None

    try:
        consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": "voting_group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True
        })
        producer = SerializingProducer({"bootstrap.servers": "localhost:9092"})
        conn = psycopg2.connect(**DATABASE_CONFIG)
        curs = conn.cursor()

        curs.execute("""
            SELECT ROW_TO_JSON(d)
            FROM (
                SELECT * FROM candidates
            ) d
        """)
        candidates = [candidate[0] for candidate in curs.fetchall()]

        if len(candidates) == 0:
            raise Exception("No candidate found in database")

        consumer.subscribe(["voters_topic"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                producer.flush()
                continue

            elif msg.error():
                print(f"An error occurred in kafka: {msg.error()}")
                break

            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)

                vote = {
                    "voter_id": voter["id"],
                    "candidate_id": chosen_candidate["id"],
                    "voting_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "vote": 1
                }

                curs.execute("""
                    INSERT INTO votes
                    VALUES (%s, %s, %s, %s)
                """, (vote["voter_id"], vote["candidate_id"], vote["voting_time"], vote["vote"]))
                conn.commit()

                producer.produce("votes_topic", vote["voter_id"], json.dumps(vote), on_delivery=delivery_report)
                producer.poll(0)

        curs.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        producer.flush()
        consumer.close()