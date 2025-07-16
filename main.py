import psycopg2
import requests
import random
import json
from confluent_kafka import SerializingProducer
from datetime import datetime

DATABASE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "admin",
    "password": "postgres",
    "database": "voting"
}
USER_API = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]

random.seed(42)

def create_table(conn, curs):
    # Create candidates table
    curs.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)

    # Create voters table
    curs.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth DATE,
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    # create votes table
    curs.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote INTEGER DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    conn.commit()

def generate_candidate_data(candidate_number):
    while True:
        try:
            result = requests.get(USER_API + "&gender=" + ("male" if candidate_number % 2 == 0 else "female"))
            data = result.json()["results"][0]

            return {
                "candidate_id": data["login"]["uuid"],
                "candidate_name": f'{data["name"]["first"]} {data["name"]["last"]}',
                "party_affiliation": PARTIES[candidate_number],
                "biography": "A brief biography of the candidate",
                "campaign_platform": "Key campaign promises and or platform",
                "photo_url": data["picture"]["large"]
            }

        except Exception as e:
            print(f"An error occurred when fetching data from randomuser API: {e}")
            print("Retrying...")

def generate_voter_data():
    while True:
        try:
            result = requests.get(USER_API)
            data = result.json()["results"][0]

            return {
                "voter_id": data["login"]["uuid"],
                "voter_name": f'{data["name"]["first"]} {data["name"]["last"]}',
                "date_of_birth": datetime.strptime(data["dob"]["date"], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")  ,
                "gender": data["gender"],
                "nationality": data["nat"],
                "registration_number": data["login"]["username"],
                "address": {
                    "street": f'{data["location"]["street"]["number"]} {data["location"]["street"]["name"]}',
                    "city": data["location"]["city"],
                    "state": data["location"]["state"],
                    "country": data["location"]["country"],
                    "postcode": data["location"]["postcode"]
                },
                "email": data["email"],
                "phone_number": data["phone"],
                "picture": data["picture"]["large"],
                "registered_age": data["registered"]["age"]
            }

        except Exception as e:
            print(f"An error occurred when fetching data from randomuser API: {e}")
            print("Retrying...")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed with error: {err}")
    else:
        print(f"Delivered message to topic {msg.topic()} [{msg.partition()}:{msg.offset()}]")

if __name__ == "__main__":
    try:
        producer = SerializingProducer({"bootstrap.servers": "localhost:9092"})
        conn = psycopg2.connect(**DATABASE_CONFIG)
        curs = conn.cursor()

        create_table(conn, curs)

        curs.execute("SELECT * FROM candidates")
        candidates = curs.fetchall()

        if len(candidates) == 0:
            for i in range(len(PARTIES)):
                while True:
                    try:
                        candidate = generate_candidate_data(i)
                        print(candidate)

                        curs.execute("""
                            INSERT INTO candidates
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (candidate["candidate_id"], candidate["candidate_name"], candidate["party_affiliation"],
                              candidate["biography"], candidate["campaign_platform"], candidate["photo_url"]))
                        conn.commit()
                        break

                    except:
                        continue

        for i in range(3000):
            while True:
                try:
                    voter = generate_voter_data()

                    curs.execute("""
                        INSERT INTO voters
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (voter["voter_id"], voter["voter_name"], voter["date_of_birth"], voter["gender"], voter["nationality"],
                          voter["registration_number"], voter["address"]["street"], voter["address"]["city"], voter["address"]["state"], voter["address"]["country"],
                          voter["address"]["postcode"], voter["email"], voter["phone_number"], voter["picture"], voter["registered_age"]))
                    conn.commit()

                    # produce message to kafka topic
                    producer.produce(topic="voters_topic", key=voter["voter_id"], value=json.dumps(voter), on_delivery=delivery_report)
                    producer.poll(0)
                    break

                except:
                    continue

        producer.flush()

        curs.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")