kvtlib -- key value maps as a function of time
==============================================

Data registration
-----------------

Data is recorded in yaml or csv format.  For example::

  domain: 'so_obsdb.sat1'
  data:
    - time_range: ['2022-03-01 06:00', '2022-03-01 07:00']
      values:
        tags: "planet,uranus"
    - time_range: ['2022-03-01 07:00', '2022-03-02 06:00']
      values:
        tags: "survey"
    - time_range: ['2022-03-02 06:00', '2022-03-02 07:00']
      values:
        tags: "planet,uranus"
    - time_range: ['2022-03-01 09:00', '2022-03-02 09:30']
      values:
        add_tags: "drone"

or::

  domain,start,end,key,value
  so_hk.labs.yale.observatory.therm1.feeds.data,"2022-02-21 6:00","2022-03-01",,
  ,,,ch1,diode232
  ,,,ch2,diode231


API
---

Build the database from local files::

  >>> db = kvtlib.build_db('data/')

To get the value associated with some key at a particular time::

  >>> db.get('so_obsdb.sat1', '2022-03-02')
  {'tags': 'survey', 'add_tags': 'drone'}

Get a history of the values associated with a key::

  >>> h = db.get_history('so_hk.labs.yale.observatory.therm1.feeds.data.ch2')
  >>> for h in h: print(h)
  ... 
  ('diode232', [1644451200.0, 1645142400.0])
  ('diode232', [1645142400.0, 1645423200.0])
  ('diode231', [1645423200.0, 1646092800.0])

