from kafi.streams.streams import Streams
import logging
logging.basicConfig(level=logging.INFO)

from kafi.kafka.cluster.cluster import Cluster
c = Cluster({"kafka": {"bootstrap.servers": "localhost:9092"}, "kafi": {"consume.timeout": 0.1, "consume.batch.size": 1000}})
# from kafi.fs.local.local import Local
# c = Local({"local": {"root.dir": "/tmp"}})

context_source = "context-data"
metric_source = "metrics-raw-telegraf-env"

# {
#     "id": "323b603d-5b5f-48d2-84fc-4e784e942289",
#     "entityType": "TOPIC",
#     "regex": "(.+)",
#     "context": [
#        {
#            "key": "application",
#            "value": "$1"
#        },
#        {
#            "key": "psp_element",
#            "value": "B109968_LAGO D&A | Team $1 - $1@mgb.ch |"
#        }
#    ]
#}
contexts = (
    Streams.source(c, context_source)
    .map(lambda r: r["value"])
)

# {
#  "fields": {
#    "counter_diff": 12198
#  },
#  "name": "kafka_server_brokertopicmetrics_bytesout_total",
#  "tags": {
#    "env": "dev",
#    "pod_name": "poc-cluster-dual-role-2",
#    "topic": "kcc-test-aggregated"
#  },
#  "timestamp": 1746799800
# }
def ts_fun(r):
    return r["timestamp"] * 1000
size_int = 1000 * 60 * 60

metrics = (
    Streams.source(c, metric_source)
    .map(lambda r: r["value"])
)

metrics_expired = (
    metrics.
    expire_tumbling(ts_fun=ts_fun, size_int=size_int, allowed_lateness_int=1000 * 60) # 1 hour, allowed_lateness_int=1 minute
)

# joined = (
#     contexts
#    .join(metrics_expired,
#          left_key_fun=lambda c: c["regex"],
#          right_key_fun=lambda m: m["tags"]["topic"],
#          project_fun=lambda c, m: {"context": {"entityType": c["entityType"],
#                                             "regex": c["regex"],
#                                             "context": c["context"]},
#                                     "metric": {"fields": m["fields"],
#                                             "name": m["name"],
#                                             "tags": {"topic": m["tags"]["topic"]}},
#                                     "timestamp": m["timestamp"]})
# )
joined = (
    contexts
   .join_pred(metrics_expired,
        #  left_key_fun=lambda c: c["regex"],
        #  right_key_fun=lambda m: m["tags"]["topic"],
         predicate_fun=lambda c, m: c["regex"] == m["tags"]["topic"],
         project_fun=lambda c, m: {"context": {"entityType": c["entityType"],
                                            "regex": c["regex"],
                                            "context": c["context"]},
                                    "metric": {"fields": m["fields"],
                                            "name": m["name"],
                                            "tags": {"topic": m["tags"]["topic"]}},
                                    "timestamp": m["timestamp"]})
)

def get_app_context(joined_r):
    ctx = joined_r["context"]
    app = "unknown_app"
    metric_name = joined_r["metric"]["name"]
    for c in ctx["context"]:
        if c["key"] == "application":
            app = c["value"]
    return app + "|" + metric_name

def combine_metrics(agg_r, r):
    """
    Generates a deep-copy of agg_r and adds the counter_diff of r to the counter_diff of agg_r.
    :param agg_r:
    :param r:
    :return:
    """
    agg_r["fields"]["counter_diff"] += r["fields"]["counter_diff"]
    agg_r["fields"]["counter_diffs"] += [r["fields"]["counter_diff"]]
    return agg_r

def agg_fun(agg_r, r):
    x = {"context": r["context"]["context"],
         "metric": combine_metrics(agg_r["metric"], r["metric"]),
         "timestamp": r["timestamp"]}
    return x

def proj_fun(key, agg):
    x = {"value": {"agg": agg,
                   "key": key}}
    return x

windowed = joined.group_by_agg_tumbling(ts_fun=ts_fun,
                             key_fun=get_app_context,
                             size_int=size_int,
                             agg_fun=agg_fun,
                             agg_initial_any={"context": dict(),
                                              "metric": {"fields": {"counter_diff": 0, "counter_diffs": []}},
                                              "timestamp": 0},
                             project_fun=proj_fun
                             ).sink(c, "test-topic")

topology = Streams.build(windowed)

#stop = Streams.start_streams(topology, progress=True)
Streams.streams(topology, progress=True)
