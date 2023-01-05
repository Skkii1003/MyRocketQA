import gzip
import json
import jsonlines

from tqdm import tqdm


def save(filename, data):
    with jsonlines.open(filename, 'a') as f:
        d = json.dumps(data)
        f.write(d)


if __name__ == '__main__':
    len = 0
    with gzip.open('corpus-enwiki-20200511-cirrussearch-parasv2.jsonl.gz', "r") as f:
        len = 36617357
        print('The total length is ' + str(len) + '\n')
        split_num = 0
        save_name = "Wiki_split/wiki"
        save_num = 0
        for i, line in enumerate(tqdm(f)):
            para_json = json.loads(line)
            if save_num >= (len / 7):
                save_num = 0
                split_num += 1
            save_path = save_name + str(split_num) + '.jsonl'
            save(save_path, para_json)
            save_num += 1
