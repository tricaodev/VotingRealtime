import numpy as np
import pandas as pd
import psycopg2
import streamlit as st
import json
from pandas import DataFrame
from streamlit_autorefresh import st_autorefresh
from datetime import datetime
from confluent_kafka import Consumer, TopicPartition
from main import DATABASE_CONFIG
import matplotlib.pyplot as plt

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
    st.text(f'Last refresh at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
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

def leading_candidate(data: DataFrame):
    st.header("Leading Candidate")
    winner = data.iloc[data["total_votes"].idxmax()]

    col1, col2 = st.columns(2)
    with col1:
        st.image(winner['photo_url'], width=200)

    with col2:
        st.header(winner["candidate_name"])
        st.subheader(winner["party_affiliation"])
        st.subheader(f'Total Votes: {winner["total_votes"]}')

    st.markdown("""---""")

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

def voting_statistics(data: DataFrame):
    data = data[["candidate_id", "candidate_name", "party_affiliation", "total_votes"]]
    st.header("Voting Statistics")
    col1, col2 = st.columns(2)

    labels = list(data["candidate_name"])
    values = list(data["total_votes"])

    # Display bar chart
    with col1:
        colors = plt.cm.viridis(np.linspace(0, 1, data.shape[0]))

        fig, ax = plt.subplots()
        ax.bar(labels, values, color=colors)
        ax.set_title("Vote Counts Per Candidate")
        ax.set_xlabel("Candidate")
        ax.set_ylabel("Total Votes")
        ax.tick_params('x', rotation=90)

        st.pyplot(fig)


    # Display pie chart
    with col2:
        fig, ax = plt.subplots()

        ax.pie(values, labels=labels, autopct='%.2f%%', startangle=90)
        ax.set_title("Candidates Votes")
        ax.axis("equal")

        st.pyplot(fig)


    # Data table
    st.table(data)

    st.markdown("""---""")


def voters_location(data: DataFrame):
    st.header("Location Of Voters")

    top_menu = st.columns(3)
    with top_menu[0]:
        is_sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=True, index=1)
        if is_sort == "Yes":
            sort_field = "state"
            direction = "⬆️"

            with top_menu[1]:
                sort_field = st.selectbox("Sort By", options=["state", "total_votes"])
            with top_menu[2]:
                direction = st.radio("Direction", options=["⬆️", "⬇️"], horizontal=True, index=0)

            data.sort_values(by=sort_field, ascending=(True if direction == "⬆️" else False), inplace=True)

    data.index = range(1, data.shape[0] + 1)

    data_container = st.container()

    bottom_menu = st.columns((4, 1, 1))
    total_pages = 0
    current_page = 1
    with bottom_menu[2]:
        page_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
        total_pages = int((data.shape[0] - 1) / page_size) + 1

    with bottom_menu[1]:
        current_page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)

    with bottom_menu[0]:
        st.markdown(f'Page **{current_page}** of **{total_pages}**')

    frames = [data.iloc[i:i+page_size, :] for i in range(0, data.shape[0], page_size)]
    data_container.dataframe(frames[current_page - 1], use_container_width=True)


def side_panel():
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(refresh_interval * 1000, key='side_panel')

    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

def get_data_from_csv(path, col_name, key):
    df = pd.read_csv(path, header=None, names=col_name)
    df = df.iloc[df.groupby(key)["total_votes"].idxmax()]
    df.reset_index(drop=True, inplace=True)

    return df


if __name__ == "__main__":
    # Prepare data
    topics = ["votes_per_candidate", "turnout_per_location"]
    for topic in topics:
        fetch_data_from_kafka(topic)

    votes_per_candidate_df = get_data_from_csv('./data/votes_per_candidate.csv', [
        "candidate_id", "candidate_name", "party_affiliation", "photo_url", "total_votes"
    ], "candidate_id")

    turnout_per_location_df = get_data_from_csv('./data/turnout_per_location.csv', [
        "state", "total_votes"
    ], "state")


    # Last refresh time
    st.title("Realtime Election Voting Dashboard")
    last_refresh_time()

    # Display total voters and candidates
    voter_count, candidate_count = get_count()
    total_metric(voter_count, candidate_count)

    # Display leading candidate
    leading_candidate(votes_per_candidate_df)

    # Display voting statistics
    voting_statistics(votes_per_candidate_df)

    # Display location of voters
    voters_location(turnout_per_location_df)

    # Side panel for auto refresh
    side_panel()


    consumer.close()
    curs.close()
    conn.close()