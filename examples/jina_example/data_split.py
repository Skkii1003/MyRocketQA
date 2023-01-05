import gzip
import json


def save(filename, data):
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(data)


if __name__ == '__main__':
    len = 0;
    with gzip.open('corpus-enwiki-20200511-cirrussearch-parasv2.jsonl.gz', "r") as f:
        for i, line in enumerate(f):
            len = i + 1;
        print('The total length is ' + str(i) + '\n')
        split_num = 0
        save_name = "wiki"
        save_num = 0
        for i, line in enumerate(f):
            para_json = json.loads(line)
            data = json.dump(para_json)
            if save_num >= (len / 5):
                save_num = 0
                split_num += 1
            save(save_name + str(split_num), data)
