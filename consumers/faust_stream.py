"""Defines trends calculations for stations"""
import logging
import faust

from dataclasses import dataclass


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic(f'cta_station_{station_name}', value_type=Station)
out_topic = app.topic(f'cta_station_{station_name}_output', partitions=1)

table = app.Table(
    f'cta_table_station_{station_name}',
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic
)


@app.agent(topic)
async def process_station(stations):
    async for station in stations:
        color = ("green" if station.green
                 else "red" if station.red
                 else "blue")

        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=color
        )

        print(f"{table[station.station_id]}")

if __name__ == "__main__":
    app.main()
