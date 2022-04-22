import os
import json
import tqdm
import datetime
import multiprocessing
import math
import argparse
from pathlib import Path


class Log:
    def __init__(self, line):
        self._json = json.loads(line)

    @property
    def timestamp(self):
        return datetime.datetime.strptime(self._json['timestamp'], '%Y-%m-%d %H:%M:%S')

    def __str__(self):
        return json.dumps(self._json)

    def __repr__(self):
        return str(self.timestamp)

    def __lt__(self, other):
        return self.timestamp < other.timestamp


class LogData:
    def __init__(self, path):
        self._index = 0
        self._list = []

        if path:
            print(f'Loading "{path}"')
            with open(path, 'r') as f:
                self._list = [Log(i) for i in f.readlines()]


    def __getitem__(self, item):
        return self._list[item]

    def __next__(self):
        if self._index >= len(self._list):
            return None

        ret_val = self._list[self._index]
        self._index += 1
        return ret_val

    def __len__(self):
        return len(self._list)


def multiprocess_main():
    file_a_path = os.path.abspath(os.path.join('json_logs', 'test_a.jsonl'))
    file_b_path = os.path.abspath(os.path.join('json_logs', 'test_b.jsonl'))

    log_a = LogData(file_a_path)
    log_b = LogData(file_b_path)

    processes = multiprocessing.cpu_count()
    chunk = math.ceil(len(log_a) / processes)

    res = [log_a[i:i + chunk] for i in range(0, len(log_a), chunk)]
    for i in res:
        print(i)


def main(file_a_path, file_b_path, output_file=None):
    log_a = LogData(file_a_path)
    log_b = LogData(file_b_path)

    whole_len = len(log_a) + len(log_b)

    res_path = os.path.join(os.path.dirname(__file__), 'merged.jsonl') if not output_file else output_file

    with open(res_path, 'w') as f:
        json_a = next(log_a)
        json_b = next(log_b)

        for _ in tqdm.tqdm(range(whole_len)):
            if json_a and json_b:
                if json_a < json_b:
                    f.write(f'{json_a}\n')
                    json_a = next(log_a)
                else:
                    f.write(f'{json_b}\n')
                    json_b = next(log_b)
            else:
                if json_a:
                    f.write(f'{json_a}\n')
                    json_a = next(log_a)
                if json_b:
                    f.write(f'{json_b}\n')
                    json_b = next(log_b)

        print(f'Saved in "{res_path}"')


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='merge two log')

    parser.add_argument(
        'input_file_1',
        metavar='<INPUT FIRST LOG>',
        type=str,
        help='path to first logs',
    )

    parser.add_argument(
        'input_file_2',
        metavar='<INPUT SECOND LOG>',
        type=str,
        help='path to second logs',
    )

    parser.add_argument(
        'output_file',
        metavar='<OUTPUT LOG>',
        nargs='?',
        default=None,
        help='path to output logs',
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()

    file_a_path = os.path.abspath(args.input_file_1)
    file_b_path = os.path.abspath(args.input_file_2)

    output_file = args.output_file

    main(file_a_path, file_b_path, output_file)
