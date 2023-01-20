import kvtlib

# Verbosely though.
kvtlib.logger.setLevel('INFO')

# Load data.
db = kvtlib.build_db('data/')


# Show some vals
print()
print('Get some values at some times:')
for (k, t) in [
        ('so_hk.labs.yale', 1645142400.1),
        ('so_obsdb.sat1', '2022-03-01 08:00'),
        ]:
    v = db.get(k, t)
    print(f'   {k}@{t} = {v}')

print()
prefix = 'so_hk.labs.yale.observatory.therm1.feeds.data.'
for k in ['ch1', 'ch2', 'ch1 asdf']:
    fullk = prefix+k
    print(f'History for {fullk}:')
    h = db.get_history(fullk)
    for _h in h:
        print(f'  (value, time_range) = {_h}')
    print()
