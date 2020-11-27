import datetime
from kafka import KafkaConsumer
import json
from json import loads
import time
import os, re, sys

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

class ESTOESnewsbulk():
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

    def process_data(self):
        for message in consumer:
            data = message.value
            data = json.loads(data)
            # print(json.dumps(data['raw']))

            param = {
            "text": data['raw']['content']
                }
            param['lang'] = self.language_detector(param)['lang']
            print(param['lang'], "  |  ", param['text'][:50])
            # break
                    #print (data['raw']['id'], index_name)

        # data yg variable param, bisa diambil dari data si kafka


       





if __name__ == '__main__':
    classname = ESTOESnewsbulk()
    classname.process_data()
