"""
Custom Kafka consumer for the malaria pipeline (named consumer group).

Role (as in the reference architecture): custom business logic / notifications.
Kafka Connect already sinks the stream into MySQL and HDFS, so this consumer
watches the stream and raises an alert when a region gets too many positive
cases in a short time.

Run one instance:      python consumer\\consumer.py A
Run a second instance: python consumer\\consumer.py B      (same group -> partitions are split)

Offset strategy (explain this in the defense):
  * enable_auto_commit=False  -> we decide when progress is saved (manual commit)
  * we commit ONLY after every message of a polled batch has been handled
  * a crash before the commit means the batch is read again  -> AT-LEAST-ONCE
  * nothing is lost, and re-reading is harmless because the logic is idempotent
    (alerts are rate-limited, MySQL writes are upserts)
"""
import os
import sys
import json
import time
from collections import defaultdict, deque

from kafka import KafkaConsumer, TopicPartition, ConsumerRebalanceListener

# ------------------------------------------------------------------ settings
BOOTSTRAP = "localhost:9092"
TOPIC = "malaria-cases"
GROUP_ID = "malaria-consumers"

NAME = sys.argv[1] if len(sys.argv) > 1 else "consumer-%d" % os.getpid()

# Set True only if Kafka Connect is NOT filling MySQL yet (Connect should do it).
WRITE_TO_MYSQL = False

ALERT_WINDOW_SEC = 60      # look at the last 60 seconds ...
ALERT_THRESHOLD = 10       # ... 10 positive cases in one region = alert
ALERT_COOLDOWN_SEC = 30    # do not repeat the same alert more often than this
MAX_RETRIES = 3            # attempts before a message goes to the dead-letter file
VERBOSE = True             # print every message (shows partition + offset live)
LAG_REPORT_SEC = 10        # print position / end offset / lag every N seconds

DEAD_LETTER_FILE = "dead_letter.jsonl"
ALERT_FILE = "alerts.log"

# ------------------------------------------------------- optional MySQL write
if WRITE_TO_MYSQL:
    import django
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dashboard_project.settings")
    django.setup()
    from django.utils.dateparse import parse_datetime
    from core.models import MalariaCase


def save_to_mysql(r):
    """Upsert on case_id: reading the same message twice never creates a duplicate."""
    MalariaCase.objects.update_or_create(
        case_id=r["case_id"],
        defaults={
            "timestamp": parse_datetime(r["timestamp"]),
            "region_id": r["region_id"], "district": r["district"],
            "gps_lat": r["gps_lat"], "gps_lon": r["gps_lon"],
            "patient_age": r["patient_age"], "gender": r["gender"],
            "symptoms": r["symptoms"], "rapid_test_result": r["rapid_test_result"],
            "body_temperature": r["body_temperature"],
            "ambient_temperature": r["ambient_temperature"],
            "humidity": r["humidity"], "rainfall_mm": r["rainfall_mm"],
        },
    )


# ------------------------------------------------------------ business logic
recent_positives = defaultdict(deque)   # region -> arrival times of recent positive cases
last_alert = {}                         # region -> time of the last alert


def check_outbreak(region, result):
    """Sliding window per region. Works because the producer keys messages by region,
    so all cases of one region land on the same partition (= the same consumer)."""
    now = time.time()
    window = recent_positives[region]
    if result == "positive":
        window.append(now)
    while window and now - window[0] > ALERT_WINDOW_SEC:
        window.popleft()
    if len(window) >= ALERT_THRESHOLD and now - last_alert.get(region, 0) > ALERT_COOLDOWN_SEC:
        last_alert[region] = now
        line = "[ALERT] %s: %d positive cases in the last %ds (instance %s)" % (
            region, len(window), ALERT_WINDOW_SEC, NAME)
        print(line)
        with open(ALERT_FILE, "a", encoding="utf-8") as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S ") + line + "\n")


def handle(message):
    record = json.loads(message.value.decode("utf-8"))
    check_outbreak(record["region_id"], record["rapid_test_result"])
    if WRITE_TO_MYSQL:
        save_to_mysql(record)
    if VERBOSE:
        print("[%s] partition=%d offset=%d key=%s region=%s result=%s" % (
            NAME, message.partition, message.offset, message.key and message.key.decode(),
            record["region_id"], record["rapid_test_result"]))


def dead_letter(message, error):
    """A message that keeps failing is written aside (never dropped silently)."""
    with open(DEAD_LETTER_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "partition": message.partition, "offset": message.offset, "error": str(error),
            "raw": message.value.decode("utf-8", errors="replace"),
        }) + "\n")
    print("[%s] DEAD LETTER partition=%d offset=%d error=%s" % (NAME, message.partition, message.offset, error))


# --------------------------------------------------------- rebalance visibility
class RebalanceLogger(ConsumerRebalanceListener):
    """Prints who owns which partitions each time the group is rebalanced."""

    def on_partitions_revoked(self, revoked):
        recent_positives.clear()   # regions may move to another consumer: restart the windows
        print("[%s] REBALANCE - partitions revoked : %s" % (NAME, sorted(p.partition for p in revoked)))

    def on_partitions_assigned(self, assigned):
        print("[%s] REBALANCE - partitions assigned: %s" % (NAME, sorted(p.partition for p in assigned)))


def print_lag(consumer):
    parts = list(consumer.assignment())
    if not parts:
        return
    ends = consumer.end_offsets(parts)
    for tp in sorted(parts, key=lambda t: t.partition):
        pos = consumer.position(tp)
        print("   [%s] partition %d: position=%d end=%d lag=%d" % (NAME, tp.partition, pos, ends[tp], ends[tp] - pos))


# ---------------------------------------------------------------------- main
consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,                 # the named consumer group
    client_id=NAME,                    # shows up in kafka-consumer-groups --describe
    enable_auto_commit=False,          # manual commit
    auto_offset_reset="earliest",      # new group -> start from the beginning of the topic
    max_poll_records=100,
    session_timeout_ms=10000,          # a dead consumer is detected after ~10 s
    key_deserializer=None,             # keep raw bytes, decoded when printed
    value_deserializer=None,           # raw bytes: bad JSON is handled in handle(), not fatal
)
consumer.subscribe([TOPIC], listener=RebalanceLogger())
print("[%s] started - group=%s topic=%s - waiting for messages..." % (NAME, GROUP_ID, TOPIC))

last_report = time.time()
try:
    while True:
        batch = consumer.poll(timeout_ms=1000)
        for tp, messages in batch.items():
            for message in messages:
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        handle(message)
                        break
                    except Exception as e:
                        print("[%s] ERROR partition=%d offset=%d attempt %d/%d: %s" % (
                            NAME, message.partition, message.offset, attempt, MAX_RETRIES, e))
                        if attempt == MAX_RETRIES:
                            dead_letter(message, e)
                        else:
                            time.sleep(attempt)

        if batch:
            consumer.commit()          # save progress only after the WHOLE batch is handled

        if time.time() - last_report >= LAG_REPORT_SEC:
            print_lag(consumer)
            last_report = time.time()

except KeyboardInterrupt:
    print("\n[%s] stopping (Ctrl+C)... uncommitted messages will be re-read by the group" % NAME)
finally:
    consumer.close()   # leaves the group cleanly so the others rebalance immediately