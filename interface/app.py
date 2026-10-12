"""Send test transactions and inspect saved scores."""

import csv
import io
import json
import os
import uuid

import altair as alt
from confluent_kafka import Producer
import pandas as pd
import psycopg
import streamlit as st

from common.schema import read_transactions_csv


BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
DATABASE_URL = os.environ["DATABASE_URL"]

st.set_page_config(page_title="Скоринг транзакций", layout="wide")
st.title("Скоринг транзакций")
send_tab, results_tab = st.tabs(["Отправить транзакции", "Результаты"])


def read_csv(uploaded_file):
    text = io.StringIO(uploaded_file.getvalue().decode("utf-8-sig"), newline="")
    return read_transactions_csv(text)


def send_rows(uploaded_file, limit: int) -> int:
    producer = Producer({"bootstrap.servers": BROKER, "acks": "all"})
    errors = []
    progress = st.progress(0)

    def on_delivery(error, _message):
        if error is not None:
            errors.append(str(error))

    sent = 0
    for row in read_csv(uploaded_file):
        if sent >= limit:
            break
        transaction_id = str(uuid.uuid4())
        payload = {"transaction_id": transaction_id, "data": row}
        producer.produce(
            TOPIC,
            key=transaction_id.encode("utf-8"),
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        sent += 1
        progress.progress(min(sent / limit, 1.0))
    if producer.flush(30) or errors:
        raise RuntimeError(f"Kafka не подтвердила отправку: {errors[:2]}")
    progress.progress(1.0)
    return sent


def load_results():
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT transaction_id, score, fraud_flag, received_at
                   FROM transaction_scores WHERE fraud_flag = 1
                   ORDER BY received_order DESC LIMIT %s""",
                (10,),
            )
            fraud_rows = cursor.fetchall()
            cursor.execute(
                """SELECT score FROM transaction_scores
                   ORDER BY received_order DESC LIMIT %s""",
                (100,),
            )
            recent_scores = [row[0] for row in cursor.fetchall()]
    return fraud_rows, recent_scores


with send_tab:
    st.write("Загрузите CSV в формате `test.csv`. Для первой проверки достаточно 100 строк.")
    uploaded = st.file_uploader("Файл с транзакциями", type="csv")
    limit = st.number_input("Сколько строк отправить", min_value=1, max_value=10000, value=100)
    if st.button("Отправить в Kafka", type="primary", disabled=uploaded is None):
        try:
            sent = send_rows(uploaded, int(limit))
            if sent:
                st.success(f"Отправлено транзакций: {sent}")
                st.caption("Результаты появятся во вкладке «Результаты» через несколько секунд.")
            else:
                st.warning("В файле нет строк с транзакциями")
        except (UnicodeError, csv.Error, ValueError, RuntimeError, BufferError) as error:
            st.error(str(error))

with results_tab:
    if st.button("Посмотреть результаты"):
        try:
            fraud_rows, recent_scores = load_results()
        except psycopg.Error:
            st.error("Не удалось прочитать результаты из PostgreSQL. Попробуйте ещё раз через несколько секунд.")
        else:
            st.subheader("Последние транзакции с флагом фрода")
            if fraud_rows:
                st.dataframe(
                    pd.DataFrame(
                        fraud_rows,
                        columns=["transaction_id", "score", "fraud_flag", "received_at"],
                    ),
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.info("Транзакций с флагом фрода пока нет")

            st.subheader("Распределение скоров последних 100 транзакций")
            if recent_scores:
                chart = (
                    alt.Chart(pd.DataFrame({"score": recent_scores}))
                    .mark_bar()
                    .encode(
                        alt.X("score:Q", bin=alt.Bin(maxbins=20), title="Скор"),
                        alt.Y("count():Q", title="Число транзакций"),
                    )
                )
                st.altair_chart(chart, use_container_width=True)
                st.caption(f"Показано транзакций: {len(recent_scores)}")
            else:
                st.info("Результатов пока нет")
