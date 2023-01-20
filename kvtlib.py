import yaml
import csv
import os
import logging
import time
import calendar

logger = logging.getLogger('sohistorylib')
_log_fmt = '%(name)s: %(levelname)s: %(message)s'
_ch = logging.StreamHandler()
_ch.setLevel(logging.DEBUG)
_ch.setFormatter(logging.Formatter(_log_fmt))
logger.addHandler(_ch)

csv.register_dialect('sohistorylib', delimiter=',', quotechar='"',
                     quoting=csv.QUOTE_ALL, lineterminator='\r\n')


def to_timestamp(timelike):
    if isinstance(timelike, list):
        return [to_timestamp(t) for t in timelike]
    if isinstance(timelike, tuple):
        return tuple([to_timestamp(t) for t in timelike])

    if isinstance(timelike, str):
        for fmt in [
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%Y-%m-%d',
        ]:
            try:
                timelike = calendar.timegm(time.strptime(timelike, fmt))
            except:
                continue
        if isinstance(timelike, str):
            raise ValueError(f'Failed to understand time string "{timelike}"')
    if isinstance(timelike, int):
        return float(timelike)
    if isinstance(timelike, float):
        return timelike
    raise ValueError(f'Expected timestamp of some kind, got "{timelike}"')


class HistDb:
    _VALUES = '.values'

    def __init__(self):
        self._data = {}

    def view(self, roots, time_range):
        pass

    def add(self, key, value, time_range):
        tokens = key.split('.')
        ref = self._data
        for t in tokens:
            if t not in ref:
                ref[t] = {}
            ref = ref[t]
        if self._VALUES not in ref:
            ref[self._VALUES] = []
        ref[self._VALUES].append((to_timestamp(time_range), value))

    def seek(self, key):
        if isinstance(key, list):
            tokens = key
        else:
            tokens = key.split('.')
        ref = self._data
        for t in tokens:
            ref = ref.get(t)
            if ref is None:
                return None
        return ref

    def _crawl_get_vals(self, leaf, timestamp, reprefix='.'):
        results = {}
        for time_range, value in leaf.get(self._VALUES, []):
            if timestamp >= time_range[0] and timestamp < time_range[1]:
                results[reprefix] = value
                break
        for k, v in leaf.items():
            if k[0] != '.':
                results.update(self._crawl_get_vals(
                    v, timestamp, reprefix=domain_join(reprefix,k)))
        return results

    def get(self, key, timestamp, exact=False, reprefix=''):
        timestamp = to_timestamp(timestamp)
        tokens = key.split('.')
        ref = self.seek(key)
        if ref is None:
            return {}
        return self._crawl_get_vals(ref, timestamp, reprefix=reprefix)

    def get_history(self, key):
        results = []
        ref = self.seek(key)
        for time_range, value in sorted(ref.get(self._VALUES, [])):
            results.append((value, time_range))
        return results

    def _browse(self, toks):
        ref = self.seek(toks)
        if ref is None:
            return []
        return [k for k in ref if k[0] != '.']

    def browse(self):
        return _Browsable(self._browse)

class _Browsable(object):
    def __init__(self, callback, toks=[]):
        self._toks = list(toks)
        self._callback = callback
        self._leafs = callback(toks)
        for leaf in self._leafs:
            setattr(self, leaf, None)
    def __repr__(self):
        return '"' + self._key() + '"'
    @property
    def _key(self):
        return '.'.join(self._toks)
    def __getattribute__(self, k):
        if k.startswith('_') or k not in self._leafs:
            return object.__getattribute__(self, k)
        return _Browsable(self._callback, self._toks + [k])
    
def build_db(input_dir):
    logger.info(f'Building database from data in {input_dir} ...')
    db = HistDb()
    for root, dirs, files in os.walk(input_dir):
        for f in sorted(files):
            full_path = os.path.join(root, f)
            if f.endswith('.csv'):
                parse_csv(db, full_path)
            elif f.endswith('.yaml') or f.endswith('.yml'):
                parse_yaml(db, full_path)
    return db

def domain_join(parent, child):
    parent = parent.strip('.')
    child = child.strip('.')
    return parent + '.' + child if parent != '' else child

def parse_data(db, data, context):
    domain = domain_join(context.get('domain', ''),
                         data.get('domain', ''))
    time_range = data.get('time_range', context['time_range'])
    for k, v in data.get('values', {}).items():
        db.add(domain_join(domain, k), v, time_range)
    for subdata in data.get('data', []):
        parse_data(db, subdata, {
            'domain': domain,
            'time_range': time_range,
            })

def parse_yaml(db, filename):
    logger.info('Parsing %s as yaml ...' % filename)
    data = yaml.safe_load(open(filename, 'r'))
    parse_data(db, data, {
        'domain': '',
        'time_range': None,
    })
    

def read_csv(filename):
    rows = []
    with open(filename, newline='') as csvfile:
        reader = csv.reader(csvfile, dialect='sohistorylib')
        for row in reader:
            rows.append(row)
    return rows

def parse_csv(db, filename):
    logger.info('Parsing %s as csv ...' % filename)
    context = {}
    header = None
    for row in read_csv(filename):
        if header is None:
            header = {c: i for i, c in enumerate(row)}
            continue
        dom, st, en, key, val = [row[header[k]]
                                 for k in ['domain', 'start', 'end', 'key', 'value']]
        if dom != '':
            context['domain'] = dom
        if st != '':
            context['time_range'] = to_timestamp([st, en])
        if key != '':
            if val == '': val = None
            parse_data(db, {'values': {key: val}}, context)
