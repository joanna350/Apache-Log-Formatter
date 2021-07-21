import json
import pickle
from os import listdir
from os.path import isfile, join
from argparse import ArgumentParser


class ApacheLogFormatter():

    json = []
    aggregate = {}
    # intermediate placeholder to process data and group/sort chronologically
    # key: int year
    #   key: int month, v: List[int]   success / not
    #                      float       total sum (GB) /average (MB)/ stdev (MB)
    #                      List[Str]   list of filenames with non-ASCII
    def __init__(self):
        input, output = argparser()
        self.read_in(input)
        self.process()
        self.export_json(output)

    @staticmethod
    def isascii(str):
        '''
        if all characters in the given string str is in ASCII, the length remains equal
        :param str:
        :return int: convert boolean into 0 for inclusion of nonASCII, 1 for else
        '''
        return int(len(str) == len(str.encode()))

    def read_in(self, filepath):
        '''
        inputs are Aapache common log format
        updates instance variable that holds relevant data by year, month
        '''

        files = [join(filepath, f) for f in listdir(filepath) if isfile(join(filepath, f))]
        for f in files:
            with open(f, 'r') as log:
                for line in log:
                    line = line.split(' ')
                    bytesize = float(line[-1].strip())
                    success = line[-2]
                    datetime = line[3]
                    dt = datetime.split('/')
                    m = int(dt[1])
                    y = int(dt[2].split(':')[0])

                    params = line[-4].split('/')
                    filename = params[-1]
                    param = params[1]

                    if y not in self.aggregate:
                        self.aggregate[y] = {}
                    if m not in self.aggregate[y]:
                        self.aggregate[y][m] = {}
                    if 'success' not in self.aggregate[y][m]:
                        self.aggregate[y][m]['success'] = [int(success[0] == '2')]
                    else:
                        self.aggregate[y][m]['success'].append(int(success[0] == '2'))

                    if 'param' not in self.aggregate[y][m]:
                        self.aggregate[y][m]['param'] = {}
                    if param not in self.aggregate[y][m]['param']:
                        self.aggregate[y][m]['param'][param] = {}
                        self.aggregate[y][m]['param'][param]= [bytesize]
                    else:
                        self.aggregate[y][m]['param'][param].append(bytesize)

                    if not self.isascii(filename):
                        if 'nonAscii' not in self.aggregate[y][m]:
                            self.aggregate[y][m]['nonAscii'] = [filename]
                        else:
                            self.aggregate[y][m]['nonAscii'].append(filename)

    def process(self):
        '''
        given
        ______
        unordered lines of apache log

        returns
        ______
        1 section per month contains
         - sorted list of top 5 params
            ranked by total gigabytes of data from requests for files of the parameter on the month
            output records the total sum along with average /std ((MB) for each parameter
         - pct of successful requests (starts with 2, 2xx) on the month
         - list of all requested filenames that had non-ASCII characters on the month
        List[dictionary], chronologically ordered
        '''
        b = self.aggregate
        line = {}
        # order by year and month
        yearkeys = sorted(list(b.keys()))
        for y in yearkeys:
            line['year'] = y
            monthkeys = sorted(list(b[y].keys()))
            for m in monthkeys:
                d = b[y][m]

                line['month'] = m
                percent_success = sum(d['success'])/len(d['success'])
                success = sum(d['success'])
                total = len(d['success'])

                line['requests'] = {}
                line['requests']['percent_success'] = percent_success
                line['requests']['success'] = success
                line['requests']['total'] = total

                if 'nonAscii' in d:
                    line['non_ascii'] = d['nonAscii']
                else:
                    line['non_ascii'] = []

                parameters = list(d['param'].keys())
                total_per_param = [] # List[tuples] where tuple is (total GB, param)
                for p in parameters:
                    total_per_param.append((sum(d['param'][p]), p))
                total_per_param.sort(key=lambda x: x[0], reverse=True)
                line['parameters'] = []
                for total_size, p in total_per_param[:5]:
                    item = {}
                    item['name'] = p
                    item['mean_MB'] = mean(d['param'][p]) / 10**6
                    if len(d['param'][p]) < 2:
                        item['stddev_MB'] = stddev(d['param'][p])
                    else:
                        item['stddev_MB'] = stddev(d['param'][p]) / 10**6
                    item['total_GB'] = total_size / 10**9
                    line['parameters'].append(item)

                self.json.append(line)
                line = {}
                line['year'] = y

    def export_json(self, output):
        '''
        export the data to the designated filepath
        '''
        jsonStr = json.dumps(self.json)
        with open(output, 'w') as jsonf:
            jsonf.write(jsonStr)
            jsonf.close()

def mean(data):
    '''
    returns the sample arithmetic mean of data
    '''
    n = len(data)
    if n < 1: raise ValueError('requires at least one data point')
    return sum(data)/len(data)

def stddev(data):
    '''
    returns the sample standard deviation
    '''
    n = len(data)
    if n < 2: return 'N/A'
    m = mean(data)
    dev = [(x-m)**2 for x in data]
    return (sum(dev)/(n - 1))**0.5

def load_pickle(filename):
    '''
    modularizes functions for large logs

    :param filename: where data is pickled
    :return: returns the unpickled data
    '''
    with open(filename, 'rb') as handle:
        b = pickle.load(handle)
    return b

def pickle_dump(d, filename):
    '''
    modularizes functions for large logs

    :param d: data to pickle
    :param filename: filename under which to store the pickled data
    '''
    with open(filename, 'wb') as handle:
        pickle.dump(d, handle, protocol=pickle.HIGHEST_PROTOCOL)

def argparser():
    '''
    receives from stdin the directory where the data to process are and the path to output
    '''
    parser = ArgumentParser()
    parser.add_argument('input', nargs= '?', action='store', default=0, type =str)
    parser.add_argument('output', nargs= '?', action='store', default=0, type =str)

    args = vars(parser.parse_args())

    return args['input'], args['output']


if __name__ == '__main__':
    ApacheLogFormatter()
