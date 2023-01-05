import gzip
import json

from tqdm import tqdm


def save(filename, data):
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(data)


if __name__ == '__main__':
    len = 0
    with gzip.open('corpus-enwiki-20200511-cirrussearch-parasv2.jsonl.gz', "r") as f:
        len = 36617357
        print('The total length is ' + str(len) + '\n')
        split_num = 0
        save_name = "wiki"
        save_num = 0
        for i, line in enumerate(tqdm(f)):
            para_json = json.loads(line)
            data = json.dumps(para_json)
            if save_num >= (len / 7):
                save_num = 0
                split_num += 1
            save(save_name + str(split_num), data)
            save_num += 1
