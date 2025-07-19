import pandas as pd
import psycopg2
import streamlit as st
import json
from streamlit_autorefresh import st_autorefresh
from datetime import datetime
from confluent_kafka import Consumer, TopicPartition
from main import DATABASE_CONFIG

conn = psycopg2.connect(**DATABASE_CONFIG)
curs = conn.cursor()

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamlit_app_group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "enable.partition.eof": True
})

def last_refresh_time():
    last_refresh = st.empty()
    last_refresh.text(f'Last refresh at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    st.markdown("""---""")

@st.cache_data(ttl=10)
def get_count():
    curs.execute("""
        SELECT COUNT(*) FROM voters
    """)
    voter_count = curs.fetchone()[0]

    curs.execute("""
        SELECT COUNT(*) FROM candidates
    """)
    candidate_count = curs.fetchone()[0]

    return voter_count, candidate_count

def total_metric(voter_count, candidate_count):
    col1, col2 = st.columns(2)

    with col1:
        st.metric("Total Voters", voter_count)

    with col2:
        st.metric("Total Candidates", candidate_count)

    st.markdown("""---""")

def show_leading_candidate():
    st.header("Leading Candidate")
    winner = leading_candidate()

    col1, col2 = st.columns(2)
    with col1:
        st.image(winner['photo_url'], width=200)

    with col2:
        st.header(winner["candidate_name"])
        st.subheader(winner["party_affiliation"])
        st.subheader(f'Total Votes: {winner["total_votes"]}')

    st.markdown("""---""")

def leading_candidate():
    fetch_data_from_kafka("votes_per_candidate")

    df = pd.read_csv('./data/votes_per_candidate.csv', header=None, names=[
        "candidate_id", "candidate_name", "party_affiliation", "photo_url", "total_votes"
    ])
    df = df.iloc[df.groupby("candidate_id")["total_votes"].idxmax()]
    df.reset_index(drop=True, inplace=True)

    result = df.iloc[df["total_votes"].idxmax()]

    return result

def fetch_data_from_kafka(topic_name):
    data = []
    topic_partition = TopicPartition(topic_name, 0)
    consumer.assign([topic_partition])

    low, high = consumer.get_watermark_offsets(topic_partition)
    committed = consumer.committed([topic_partition])
    last_offset = committed[0].offset

    while True:
        if last_offset == high:
            break

        msg = consumer.poll(1)
        if msg is None:
            continue

        if msg.error():
            break

        result = json.loads(msg.value().decode('utf-8'))
        data.append(result)

    df = pd.DataFrame(data)
    df.to_csv(f'./data/{topic_name}.csv', index=False, header=False, mode='a')

def side_panel():
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(refresh_interval * 1000, key='side_panel')


if __name__ == "__main__":
    st.title("Realtime Election Voting Dashboard")
    last_refresh_time()

    # Display total voters and candidates
    voter_count, candidate_count = get_count()
    total_metric(voter_count, candidate_count)

    # Display leading candidate
    show_leading_candidate()



    # Side panel for auto refresh
    side_panel()



    consumer.close()
    curs.close()
    conn.close()