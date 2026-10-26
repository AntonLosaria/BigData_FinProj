"""
Kafka Producer Template for Streaming Data Dashboard
STUDENT PROJECT: Big Data Streaming Data Producer

This is a template for students to build a Kafka producer that generates and sends
streaming data to Kafka for consumption by the dashboard.

DO NOT MODIFY THE TEMPLATE STRUCTURE - IMPLEMENT THE TODO SECTIONS
"""

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import requests
from dotenv import load_dotenv

# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError

try:  # kafka-python 2.x
    from kafka.errors import NoBrokersAvailable
except ImportError:  # newer releases renamed/removed it
    NoBrokersAvailable = KafkaError
    
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://api.weatherapi.com/v1/current.json"

# metric_type -> (field in API "current" object, unit)
METRICS = {
    "temperature": ("temp_c", "celsius"),
    "humidity": ("humidity", "percent"),
    "pressure": ("pressure_mb", "hPa"),
    "wind_speed": ("wind_kph", "kph"),
    "uv_index": ("uv", "index"),
}

DEFAULT_CITIES = "Quezon City,London,Tokyo"


class StreamingDataProducer:
    """
    Kafka producer that pulls live weather data from WeatherAPI.
    Handles Kafka connection, API fetching, serialization and message sending.
    """

    def __init__(self, bootstrap_servers: str, topic: str, api_key: str, cities: List[str]):
        """
        Parameters:
        - bootstrap_servers: Kafka broker addresses (e.g., "localhost:9092")
        - topic: Kafka topic to produce messages to
        - api_key: WeatherAPI key
        - cities: list of city names to query
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.api_key = api_key
        self.cities = cities
        self.session = requests.Session()

        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
            'acks': 'all',
            'retries': 5,
        }

        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    #API
    def fetch_city(self, city: str) -> Optional[Dict[str, Any]]:
        """Call WeatherAPI for one city. Returns parsed JSON or None on failure."""
        try:
            resp = self.session.get(
                API_URL,
                params={"key": self.api_key, "q": city, "aqi": "no"},
                timeout=10,
            )
            if resp.status_code in (401, 403):
                print(f"ERROR: API key rejected ({resp.status_code}): {resp.text[:120]}")
                return None
            if resp.status_code == 429:
                print("WARNING: API rate limit hit (429). Backing off 30 s.")
                time.sleep(30)
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            print(f"WARNING: API timeout for '{city}'")
        except requests.exceptions.RequestException as e:
            print(f"WARNING: API request failed for '{city}': {e}")
        except ValueError:
            print(f"WARNING: Invalid JSON from API for '{city}'")
        return None

    def generate_sample_data(self) -> List[Dict[str, Any]]:
        """
        Fetch real weather data for every configured city and flatten it into
        one record per metric (schema documented at top of file).
        """
        records: List[Dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

        for city in self.cities:
            payload = self.fetch_city(city)
            if not payload or "current" not in payload:
                continue

            loc = payload.get("location", {})
            cur = payload["current"]
            location = f"{loc.get('name', city)}, {loc.get('country', '')}".strip(", ")
            slug = re.sub(r"[^a-z0-9]+", "_", loc.get("name", city).lower()).strip("_")

            for metric_type, (field, unit) in METRICS.items():
                value = cur.get(field)
                if value is None:
                    continue
                records.append({
                    "timestamp": now,
                    "value": float(value),
                    "metric_type": metric_type,
                    "sensor_id": f"weatherapi_{slug}",
                    "location": location,
                    "unit": unit,
                    "condition": cur.get("condition", {}).get("text"),
                    "last_updated": cur.get("last_updated"),
                })
        return records

    # ---------------------------------------------------------------- Kafka
    def serialize_data(self, data: Dict[str, Any]) -> Optional[bytes]:
        """Serialize the record to JSON bytes for Kafka transmission."""
        try:
            return json.dumps(data).encode('utf-8')
        except (TypeError, ValueError) as e:
            print(f"ERROR: Serialization failed: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        """Send one record to Kafka. Returns True on success."""
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False

        serialized_data = self.serialize_data(data)
        if not serialized_data:
            return False

        try:
            future = self.producer.send(
                self.topic,
                key=data["sensor_id"].encode("utf-8"),
                value=serialized_data,
            )
            future.get(timeout=10)  # wait for broker acknowledgement
            return True
        except KafkaError as e:
            print(f"Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during send: {e}")
            return False

    def produce_stream(self, interval: float = 15.0, duration: int = None):
        """
        Main streaming loop: every `interval` seconds fetch all cities and publish.

        Parameters:
        - interval: seconds between API polls (default 15 -> matches dashboard refresh)
        - duration: total runtime in seconds (None for infinite)
        """
        print(f"Starting producer: every {interval}s, cities={self.cities}, "
              f"duration: {duration or 'infinite'}")

        start_time = time.time()
        message_count = 0

        try:
            while True:
                if duration and (time.time() - start_time) >= duration:
                    print(f"Reached duration limit of {duration} seconds")
                    break

                cycle_start = time.time()
                records = self.generate_sample_data()
                sent = sum(1 for r in records if self.send_message(r))
                message_count += sent

                stamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{stamp}] sent {sent}/{len(records)} messages "
                      f"(total {message_count})")

                time.sleep(max(0.0, interval - (time.time() - cycle_start)))

        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        except Exception as e:
            print(f"Streaming error: {e}")
        finally:
            self.close()
            print(f"Producer stopped. Total messages sent: {message_count}")

    def close(self):
        """Flush and close the Kafka producer."""
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")


def parse_arguments():
    """Command-line arguments."""
    parser = argparse.ArgumentParser(description='WeatherAPI -> Kafka Streaming Producer')

    parser.add_argument('--bootstrap-servers', type=str,
                        default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                        help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', type=str,
                        default=os.getenv('KAFKA_TOPIC', 'streaming-data'),
                        help='Kafka topic to produce to (default: streaming-data)')
    parser.add_argument('--interval', type=float, default=15.0,
                        help='Seconds between API polls (default: 15)')
    parser.add_argument('--duration', type=int, default=None,
                        help='Run duration in seconds (default: infinite)')
    parser.add_argument('--cities', type=str,
                        default=os.getenv('WEATHER_CITIES', DEFAULT_CITIES),
                        help='Comma-separated city list')
    parser.add_argument('--api-key', type=str, default=os.getenv('WEATHER_API_KEY'),
                        help='WeatherAPI key (or set WEATHER_API_KEY in .env)')

    return parser.parse_args()


def main():
    print("=" * 60)
    print("WEATHERAPI -> KAFKA STREAMING PRODUCER")
    print("=" * 60)

    args = parse_arguments()

    if not args.api_key:
        print("ERROR: No API key. Set WEATHER_API_KEY in .env or pass --api-key")
        return

    cities = [c.strip() for c in args.cities.split(",") if c.strip()]

    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        api_key=args.api_key,
        cities=cities,
    )
    if producer.producer is None:
        print("Cannot start: Kafka is not reachable. Start the broker and retry.")
        return

    try:
        producer.produce_stream(interval=args.interval, duration=args.duration)
    except Exception as e:
        print(f"Main execution error: {e}")
    finally:
        print("Producer execution completed")


if __name__ == "__main__":
    main()
