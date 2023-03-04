from itertools import combinations
from datetime import datetime, timedelta
import os
import json
import random
import time
from tracemalloc import start
from zlib import crc32
from locust import HttpUser, task, between, events
import logging

config = {}

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    global config
    config = json.load(open("loadtest_config.json"))
    print("config: ", config)


@events.spawning_complete.add_listener
def on_spawning_complete(user_count):
    print("users", user_count)
    print("config: ", config)

alphabet = "abcdefghijklmnopqrstuvwxyz"

# we read a couple hundred nouns from a file, make all the combinations of 2-word nouns
# then order them by their checksum and extract the first 1000
fnouns = open("common_nouns.txt").read().splitlines()
nouns = sorted(
    [n[0] + "_" + n[1] for n in combinations(fnouns, 2)],
    key=lambda x: crc32(bytes(x, "utf-8")),
)[:1000]

print(len(nouns), nouns[:10])


def random_int(max=1000):
    return random.randrange(0, max)


def random_float(max=10.0):
    return random.random() * max


def random_high_cardinality_string():
    return "".join(random.choices(alphabet, k=20))


def random_low_cardinality_string():
    return "word_" + random.choice(nouns)

# returns a random hex-ified string of bytes (length is 2*nbytes)
def random_id(nbytes):
    return ''.join([f'{x:x}' for x in random.randbytes(nbytes)])


value_functors = [
    random_int,
    random_float,
    random_high_cardinality_string,
    random_low_cardinality_string,
]

# we want to have a list of nouns where each one has the same type consistently
# across runs, so we create a list of nouns, hash them, and use the hash to index into
# the list of value_functors
fields = dict(
    ((x, value_functors[crc32(bytes(x, "utf-8")) % len(value_functors)]) for x in nouns)
)


def gen_one_event(parent_trace_info=None, name="test event"):
    # these are the fields we always have on an event
    starttime = datetime.utcnow()
    time.sleep(random_float(0.3))
    stoptime = datetime.utcnow()

    event = {
        "name": name,
        "tool": "query_test",
        "duration_ms": (stoptime - starttime)/timedelta(microseconds=1000),
    }
    event.update(config["extras"])
    if parent_trace_info:
        traceinfo = parent_trace_info.copy()
        traceinfo["trace.parent_id"] = parent_trace_info["trace.span_id"]
        traceinfo["trace.span_id"] = random_id(8)
        traceinfo["timestamp"] = starttime.isoformat(timespec="microseconds")
        event.update(traceinfo)

    # these are randomly generated values but a given column will always have the same type
    for i in range(config["event_width"] - len(event)):
        col = nouns[i % len(nouns)]
        event[col] = fields[col]()

    big_trace_size_in_k = 20
    if random_float(100) < 1.0:
        for i in range(big_trace_size_in_k):
            name = f"long_column_{i}"
            data = "houselager"*100 # 1000 characters
            event[name]=data
        event["metadata"]="foobar"
        # print("sending giant span")

    return [event]


def gen_one_trace(name="refinery test trace"):
    # these are the fields we always have on a trace
    starttime = datetime.utcnow()
    trace_info = {
        "trace.trace_id":random_id(16),
        "trace.span_id": random_id(8),
        "timestamp": starttime.isoformat(timespec="microseconds"),
    }
    time.sleep(random_float(0.1))
    nspans = random_int(max=15)+1
    spans = []
    for i in range(nspans):
        spans.extend(gen_one_event(parent_trace_info=trace_info, name=f"test event {i}"))
        time.sleep(random_float(0.1))
    stoptime = datetime.utcnow()

    # we send a mix of status codes for testing refinery's data type coercion
    status_type, status_code = random.choice([
        ("string", "200"),
        ("string", "500"),
        ("int", 200),
        ("int", 500),
    ])

    trace = {
        "name": name,
        "tool": "query_test",
        "duration_ms": (stoptime - starttime)/timedelta(microseconds=1000),
        "http.status_type": status_type,
        "http.status_code": status_code,
        "http.method":"GET",
        "service.name": random.choice(["service-a", "service-b", "service-c"]),
    }
    trace.update(config["extras"])
    trace.update(trace_info)

    # these are randomly generated values but a given column will always have the same type
    for i in range(config["event_width"] - len(trace)):
        col = nouns[i % len(nouns)]
        trace[col] = fields[col]()

    # let's "fail" 10% of traces
    if random_float() < 1:
        trace["is_error"] = True

    spans.append(trace)
    return spans

class HoneycombBatchUser(HttpUser):
    wait_time = between(0.5, 1.5)
    # we need to load config here so that we can specify host; we also reload it
    # whenever a test starts, so that you can modify the config and just run a new
    # test without restarting locust.
    config = json.load(open("loadtest_config.json"))
    host = config["hosts"][config["host"]]
    logging.info("Starting user")

    @task(1)
    def send_batch(self):

        headers = {
            "X-Honeycomb-Team": config["LIBHONEY_WRITE_KEY"],
            "Content-Type": "application/json",
        }

        url = os.path.join(config["batch_endpoint"], config["dataset"])
        j = [
            {
                "time": d["timestamp"]+"Z",
                "data": d,
            }
            for d in gen_one_trace()
        ]
        # print(json.dumps(j, sort_keys=True, indent=2))
        self.client.post(url, headers=headers, json=j)
