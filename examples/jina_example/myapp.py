import gzip
import json
import os
import sys
from pathlib import Path

from jina import Document, Flow
from tqdm import tqdm


def config():
    os.environ.setdefault('JINA_USE_CUDA', 'True')
    os.environ.setdefault('JINA_PORT_EXPOSE', '8886')
    os.environ.setdefault('JINA_WORKSPACE', './workspace')

def index(file_name):
    def readFile(file_name):
        with gzip.open(file_name, "r") as f:
            for i, line in enumerate(f):
                try:
                    para_json = json.loads(line)
                    doc = Document(
                        id=f'{i}',
                        tags={'docId': para_json["docid"],
                              'secId': para_json["secid"],
                              'headerId': para_json["headerid"],
                              'paraId': para_json["para_id"],
                              'title': para_json["title"],
                              'section': para_json.get("section") or "",
                              'subsection': " :: ".join(para_json.get("headers")) or "",
                              'text': para_json["para"]
                              }
                    )
                    yield doc
                except:
                    print(f'skip line {i}')
                    continue

    f = Flow().load_config('flows/index.yml')
    with f:
        f.post(on='/index', inputs=readFile(file_name), show_progress=True, request_size=32)

def query():
    def print_topk(resp):
        for doc in resp.docs:
            print(doc)
            doc = Document(doc)
            print(f'🤖 Answers:')
            for m in doc.matches:
                print(f'\t{m.tags["title"]}')
                print(f'\t{m.tags["para"]}')
                print(f'-----')

    f = Flow().load_config('flows/query.yml')
    with f:
        f.protocol = 'grpc'
        print(f'running questions in strategyqa_test.json...\n')
        with open("strategyqa_dataset/strategyqa_test.json", "r") as q:
            for i,line in enumerate(q):
                question = json.loads(line)
                text = question["question"]
                f.post(on='/search', inputs=[Document(content=text), ], on_done=print_topk)


def query_cli():
    def print_topk(resp):
        for doc in resp.docs:
            print(doc)
            doc = Document(doc)
            print(f'🤖 Answers:')
            for m in doc.matches:
                print(f'\t{m.tags["title"]}')
                print(f'\t{m.tags["para"]}')
                print(f'------------------')

    f = Flow().load_config('flows/query.yml')
    with f:
        f.protocol = 'grpc'
        print(f'🤖 Hi there, please ask me questions.\n'
              'For example, "Can a honey bee sting a human more than once?"\n')
        while True:
            text = input('Question: (type `\q` to quit)')
            if text == '\q' or not text:
                return
            f.post(on='/search', inputs=[Document(content=text), ], on_done=print_topk)

def main(task):
    config()
    if task == 'index':
        if Path('./workspace').exists():
            print('./workspace exists, please deleted it if you want to reindexi')
        data_fn = sys.argv[2] if len(sys.argv) >= 3 else 'toy_data/test.tsv'
        print(f'indexing {data_fn}')
        index(data_fn)
    elif task == 'query_cli':
        query_cli()
    elif task == 'query':
        query()


if __name__ == '__main__':
    task = sys.argv[1]
    main(task)

