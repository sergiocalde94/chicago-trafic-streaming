"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088/ksql"

KSQL_STATEMENT = """
    CREATE TABLE turnstile (
        station_id INT,
        station_name VARCHAR,
        line VARCHAR
    ) WITH (
        kafka_topic='org.chicago.cta.turnstiles',
        value_format='avro',
        key='station_id'
    );
    CREATE TABLE turnstile_summary
    WITH (value_format='JSON') AS
        SELECT
        station_id,
        count(station_id) AS count
        FROM turnstile
        GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        KSQL_URL,
        headers={
            "Content-Type": "application/vnd.ksql.v1+json; charset=utf-8",
            "Accept": "application/vnd.ksql.v1+json"
        },
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties":
                    {"ksql.streams.auto.offset.reset": "earliest"}
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
