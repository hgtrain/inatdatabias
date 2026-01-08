import json, logging, requests, time
from kafka import KafkaProducer
from root import ROOT_PATH

SERVER = "localhost:9092"
OBS_TOPIC_NAME = "observations"
PERPAGE = 200

log = logging.getLogger(__name__)
logging.basicConfig(
    filename=f"{ROOT_PATH}/logs/kafka_broker.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s [at %(filename)s:%(funcName)s:%(lineno)s]",
    filemode="w"
)

producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(0, 11),
    retries=5
)

def get_obs(url, tryagain=True):
    try:
        rsp = requests.get(url, timeout=(5, 15))
        if rsp.status_code != 200:
            log.error("Received %s status response from API", rsp.status_code)
            return None
        rsp_json = rsp.json()
        return (rsp_json["total_results"], rsp_json["results"])
    # except requests.Timeout as e:
    except BaseException as e:
        if tryagain:
            time.sleep(2)
            return get_obs(url, False)
        else:
            log.error("Could not connect to API: %s", e, exc_info=True)
            return None

def publish_to_kafka(l: list[dict]):
    def on_send_success(rmd):
        log.info(f"Sent observation to topic {rmd.topic} on partition {rmd.partition} with offset {rmd.offset}")
    def on_send_error(e):
        log.error("Failed to send observation to producer: %s", e, exc_info=True)
    for i in l:
        producer.send(OBS_TOPIC_NAME, value=i).add_callback(on_send_success).add_errback(on_send_error)
        id = i["id"]
        print(f"sent {id}")
    producer.flush()
    print("flushed")

def get_year(year):
    url = f"https://api.inaturalist.org/v1/observations?identified=true&mappable=true&verifiable=true&place_id=51&year={year}&geoprivacy=open&taxon_geoprivacy=open&quality_grade=research&page=1&per_page={PERPAGE}&order=asc&order_by=created_at"
    rsp = get_obs(url)
    if not rsp:
        return None
    total_results, results = rsp
    last_id = results[-1]["id"]
    print(total_results)
    print(len(results))
    print(last_id)
    publish_to_kafka(results)
    print("published")

    while total_results != 0:
        url = f"https://api.inaturalist.org/v1/observations?identified=true&mappable=true&verifiable=true&place_id=51&year={year}&geoprivacy=open&taxon_geoprivacy=open&id_above={last_id}&quality_grade=research&page=1&per_page={PERPAGE}&order=asc&order_by=created_at"
        rsp = get_obs(url)
        if not rsp:
            return last_id
        total_results, results = rsp
        last_id = results[-1]["id"]
        print(total_results)
        print(len(results))
        print(last_id)
        publish_to_kafka(results)

def listen(start_id):
    last_id = start_id
    while True:
        url = f"https://api.inaturalist.org/v1/observations?identified=true&mappable=true&verifiable=true&place_id=51&geoprivacy=open&taxon_geoprivacy=open&id_above={last_id}&quality_grade=research&page=1&per_page={PERPAGE}&order=asc&order_by=created_at"
        rsp = get_obs(url)
        if not rsp:
            return last_id
        total_results, results = rsp
        if (total_results == 0):
            break
        last_id = results[-1]["id"]
        publish_to_kafka(results)

def start():
    get_year(2019)
    get_year(2020)
    last = get_year(2025)
    listen(last)

# start()

# listen(333801940)

def on_send_success(rmd):
    log.info(f"Sent observation to topic {rmd.topic} on partition {rmd.partition} with offset {rmd.offset}")
def on_send_error(e):
    log.error("Failed to send observation to producer: %s", e, exc_info=True)
i = 0
while True:
    i += 1
    d = {"id": i}
    producer.send(OBS_TOPIC_NAME, value=d).add_callback(on_send_success).add_errback(on_send_error)
    print(f"sent {i}")

