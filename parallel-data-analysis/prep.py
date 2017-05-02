import os
import shutil
from glob import glob
from fakestockdata import generate_stocks
import pandas as pd
import json

def get_data_dir():
	here = os.path.dirname(__file__)
	data = os.path.join(here, 'data')
	return data

def make_data_dir():
	data = get_data_dir()
	if not os.path.exists(data):
	    os.mkdir(data)

def make_minute_data():
	data = get_data_dir()
	minute = os.path.join(data, 'minute')
	if not os.path.exists(minute):
		os.mkdir(minute)
		generate_stocks(freq=pd.Timedelta(seconds=120),
		                start=pd.Timestamp('2010-01-01'),
		                directory=minute)

def convert_to_json(d):
    filenames = sorted(glob(os.path.join(d, '*')))[-365:]
    with open(d.replace('minute', 'json') + '.json', 'w') as f:
        for fn in filenames:
            df = pd.read_csv(fn)
            for rec in df.to_dict(orient='records'):
                json.dump(rec, f)
                f.write(os.linesep)
    print("Finished JSON: %s" % d)

def write_json_files():
	data = get_data_dir()
	js = os.path.join(data, 'json')
	if not os.path.exists(js):
		from concurrent.futures import ProcessPoolExecutor
		minute = os.path.join(data, 'minute')
		os.mkdir(js)
		directories = sorted(glob(os.path.join(minute, '*')))

		e = ProcessPoolExecutor()
		list(e.map(convert_to_json, directories))


def main():
	make_data_dir()
	make_minute_data()
	write_json_files()


if __name__ == "__main__":
	main()