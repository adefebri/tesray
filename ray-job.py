import datetime
from collections import Counter
from kafka import KafkaConsumer
import json
from json import loads
import time
import os, re, sys
import ray

from datetime import datetime

consumer = KafkaConsumer(
    'annotation-online-news',
    bootstrap_servers=['datanode04.hdp03.bt:6667',
                       'datanode05.hdp03.bt:6667',
                       'datanode06.hdp03.bt:6667',
                       'datanode07.hdp03.bt:6667',
                       'datanode13.hdp03.bt:6667',
                       'datanode14.hdp03.bt:6667',
                       'datanode15.hdp03.bt:6667',
                       'datanode16.hdp03.bt:6667'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='logging-annotation')

path_this = os.path.dirname(os.path.abspath(__file__))
path_config = os.path.abspath(os.path.join(path_this, "config"))
path_library = os.path.abspath(os.path.join(path_this, "library"))
path_project = os.path.abspath(os.path.join(path_this, ".."))
sys.path.append(path_this)
sys.path.append(path_project)
sys.path.append(path_library)

from dl.engine.language_detector.lang_classifier import LanguageClassifier


@ray.remote
def gethostname(x):
    import platform
    import time
    time.sleep(0.01)
    return x + (platform.node(), )


def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        num_nodes = len(ray.nodes())
        if num_nodes < expected:
            print("{} nodes have joined so far, waiting for {} more.".format(
                num_nodes, expected - num_nodes))
            sys.stdout.flush()
            time.sleep(1)
        else:
            break


def main():
    wait_for_nodes(4)

    def __init__(self, *args, **kwargs):
        self.obj_lang = LanguageClassifier()
        self._RE_SENTENCES = re.compile(
            r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=[\.\?])\s", flags=re.I
        )
        self._TAG_RE = re.compile(r"<[^>]+>")
        self._HASHTAG = re.compile("(?:^|\s)[##]{1}(\w+)", re.UNICODE)
        self._MENTION = re.compile("(?:^|\s)[@]{1}([^\s#:<>[\]|{}]+)", re.UNICODE)
        self.LONG_NEWLINE = re.compile(r"[\n]+", flags=re.I)

        print("Success")

    def language_detector(self, params):
        clean_text = self._TAG_RE.sub(" ", params["text"])
        sents = self._RE_SENTENCES.sub("\n", clean_text)
        sents = sents.split("\n")[:5]
        langs = list()
        final_lang = self.obj_lang.classify(" ".join(sents))[0]
        final_lang = final_lang if final_lang != "ms" else "my"
        # print(final_lang)
        return {"lang": final_lang}

    for message in consumer:
        data = message.value
        data = json.loads(data)
        # print(json.dumps(data['raw']))

        param = {
        "text": data['raw']['content']
            }
        param['lang'] = self.language_detector(param)['lang']
        print(param['lang'], "  |  ", param['text'][:50])

  



if __name__ == "__main__":
    # NOTE: If you know you're running this on the head node, you can just
    # use "localhost" here.
    # redis_host = "localhost"
    if ("RAY_HEAD_SERVICE_HOST" not in os.environ
            or os.environ["RAY_HEAD_SERVICE_HOST"] == ""):
        raise ValueError("RAY_HEAD_SERVICE_HOST environment variable empty."
                         "Is there a ray cluster running?")
    redis_host = os.environ["RAY_HEAD_SERVICE_HOST"]
    print(redis_host)
    ray.init(address=redis_host + ":6380")
    main()
    
