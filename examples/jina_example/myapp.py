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
            for i, line in enumerate(tqdm(f)):
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


if __name__ == '__main__':
    task = sys.argv[1]
    main(task)
