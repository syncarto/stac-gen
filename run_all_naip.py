import os
import json

all_cases = [
    ('al', '2015', '100cm'),
    ('al', '2017', '100cm'),
    ('al', '2013', '100cm'),
    ('ar', '2017', '100cm'),
    ('ar', '2013', '100cm'),
    ('ar', '2015', '100cm'),
    ('az', '2017', '60cm'),
    ('az', '2015', '100cm'),
    ('az', '2013', '100cm'),
    ('ca', '2016', '60cm'),
    ('ca', '2014', '100cm'),
    ('ca', '2012', '100cm'),
    ('co', '2017', '100cm'),
    ('co', '2015', '100cm'),
    ('co', '2013', '100cm'),
    ('ct', '2012', '100cm'),
    ('ct', '2016', '60cm'),
    ('ct', '2014', '100cm'),
    ('de', '2015', '100cm'),
    ('de', '2017', '100cm'),
    ('de', '2013', '100cm'),
    ('fl', '2017', '100cm'),
    ('fl', '2015', '100cm'),
    ('fl', '2013', '100cm'),
    ('ga', '2017', '100cm'),
    ('ga', '2015', '100cm'),
    ('ga', '2013', '100cm'),
    ('ia', '2014', '100cm'),
    ('ia', '2017', '100cm'),
    ('ia', '2013', '100cm'),
    ('ia', '2015', '100cm'),
    ('id', '2017', '100cm'),
    ('id', '2015', '100cm'),
    ('id', '2013', '50cm'),
    ('il', '2014', '100cm'),
    ('il', '2017', '100cm'),
    ('il', '2012', '100cm'),
    ('il', '2015', '100cm'),
    ('in', '2014', '100cm'),
    ('in', '2016', '60cm'),
    ('in', '2012', '100cm'),
    ('ks', '2015', '100cm'),
    ('ks', '2017', '100cm'),
    ('ks', '2014', '100cm'),
    ('ks', '2012', '100cm'),
    ('ky', '2014', '100cm'),
    ('ky', '2012', '100cm'),
    ('ky', '2016', '60cm'),
    ('la', '2017', '100cm'),
    ('la', '2015', '100cm'),
    ('la', '2013', '100cm'),
    ('ma', '2016', '60cm'),
    ('ma', '2014', '100cm'),
    ('ma', '2012', '100cm'),
    ('md', '2013', '100cm'),
    ('md', '2017', '100cm'),
    ('md', '2015', '100cm'),
    ('me', '2013', '100cm'),
    ('me', '2015', '100cm'),
    ('mi', '2014', '100cm'),
    ('mi', '2016', '60cm'),
    ('mi', '2012', '100cm'),
    # ('mn', '2013', '100cm'),  # mjh commenting this out temporarily since it already ran
    ('mn', '2015', '100cm'),
    ('mn', '2017', '100cm'),
    ('mo', '2012', '100cm'),
    ('mo', '2016', '60cm'),
    ('mo', '2014', '100cm'),
    ('ms', '2016', '60cm'),
    ('ms', '2012', '100cm'),
    ('ms', '2014', '100cm'),
    ('mt', '2015', '100cm'),
    ('mt', '2017', '60cm'),
    ('mt', '2013', '100cm'),
    ('nc', '2016', '100cm'),
    ('nc', '2014', '100cm'),
    ('nc', '2012', '100cm'),
    ('nd', '2014', '100cm'),
    ('nd', '2016', '60cm'),
    ('nd', '2017', '60cm'),
    ('nd', '2012', '100cm'),
    ('nd', '2015', '100cm'),
    ('ne', '2012', '100cm'),
    ('ne', '2014', '100cm'),
    ('ne', '2016', '60cm'),
    ('nh', '2014', '100cm'),
    ('nh', '2012', '100cm'),
    ('nh', '2016', '60cm'),
    ('nj', '2017', '100cm'),
    ('nj', '2013', '100cm'),
    ('nj', '2015', '100cm'),
    ('nm', '2016', '100cm'),
    ('nm', '2011', '100cm'),
    ('nm', '2014', '100cm'),
    ('nv', '2017', '100cm'),
    ('nv', '2013', '100cm'),
    ('nv', '2015', '100cm'),
    ('ny', '2017', '100cm'),
    ('ny', '2013', '100cm'),
    ('ny', '2015', '50cm'),
    ('oh', '2017', '100cm'),
    ('oh', '2013', '100cm'),
    ('oh', '2015', '100cm'),
    ('ok', '2017', '100cm'),
    ('ok', '2015', '100cm'),
    ('ok', '2013', '100cm'),
    ('or', '2014', '100cm'),
    ('or', '2016', '100cm'),
    ('or', '2012', '100cm'),
    ('pa', '2013', '100cm'),
    ('pa', '2015', '100cm'),
    ('pa', '2017', '100cm'),
    ('ri', '2012', '100cm'),
    ('ri', '2014', '100cm'),
    ('ri', '2016', '60cm'),
    ('sc', '2017', '100cm'),
    ('sc', '2015', '100cm'),
    ('sc', '2013', '100cm'),
    ('sd', '2014', '100cm'),
    ('sd', '2016', '60cm'),
    ('sd', '2012', '100cm'),
    ('tn', '2012', '100cm'),
    ('tn', '2016', '60cm'),
    ('tn', '2014', '100cm'),
    ('tx', '2016', '100cm'),
    ('tx', '2014', '100cm'),
    ('tx', '2012', '100cm'),
    ('ut', '2014', '100cm'),
    ('ut', '2011', '100cm'),
    ('ut', '2016', '100cm'),
    ('va', '2016', '100cm'),
    ('va', '2012', '100cm'),
    ('va', '2014', '100cm'),
    ('vt', '2012', '100cm'),
    ('vt', '2016', '60cm'),
    ('vt', '2014', '100cm'),
    ('wa', '2017', '100cm'),
    ('wa', '2013', '100cm'),
    ('wa', '2015', '100cm'),
    ('wi', '2015', '100cm'),
    ('wi', '2017', '100cm'),
    ('wi', '2013', '100cm'),
    ('wv', '2014', '100cm'),
    ('wv', '2011', '100cm'),
    ('wv', '2016', '100cm'),
    ('wy', '2017', '100cm'),
    ('wy', '2012', '100cm'),
    ('wy', '2015', '50cm'),
]

LOG_DIR = '/logs'
if not os.path.isdir(LOG_DIR):
    os.makedirs(LOG_DIR)

with open('naip_config.json') as f:
    config_template = json.loads(f.read())

config_dump_dir = 'naip_configs'
if not os.path.isdir(config_dump_dir):
    os.makedirs(config_dump_dir)

# group by state
cases_by_state = dict()
for case in all_cases:
    state, year, res = case
    if state not in cases_by_state:
        cases_by_state[state] = []
    cases_by_state[state].append(case)

# now for each state, sort by year so we can tag the "latest"
for state in cases_by_state:
    cases_by_state[state] = sorted(cases_by_state[state], key=lambda a: int(a[1]))

LIMIT_STATES = ['mn']
for state in cases_by_state:
    for case in cases_by_state[state]:
        if state not in LIMIT_STATES:
            continue

        _, year, res = case

        config_filename = os.path.join(config_dump_dir, '{}_{}_{}_naip_config.json'.format(state, year, res))

        config = config_template.copy()

        # replace config variables as needed for this state/year/res
        config['BUCKET_PREFIX'] = '{}/{}/{}/rgb/'.format(state, year, res)
        config['CATALOG_ID'] = 'NAIP_{}_{}_{}'.format(state, year, res)
        config['CATALOG_DESCRIPTION'] = 'NAIP_{}_{}_{}'.format(state, year, res)
        config['COLLECTION_METADATA']['id'] = '{}/{}/{}/rgb'.format(state, year, res)
        config['COLLECTION_METADATA']['description'] = 'NAIP {} {} {}'.format(state, year, res)
        config['COLLECTION_METADATA']['title'] = 'NAIP {} {} {}'.format(state, year, res)

        # "if this case is the latest case in the list"
        if case == cases_by_state[state][-1]:
            # Ideally we would put this as a separate property rather than
            # using the item "collection" property here, but that would
            # require forking sat-api to index another property
            config['ITEM_COLLECTION_PROPERTY'] = 'NAIP_LATEST'

        config['DISABLE_STAC_LINT'] = True

        with open(config_filename, 'w') as f:
            f.write(json.dumps(config, indent=2))

        log_file = os.path.join(LOG_DIR, '{}.txt'.format(os.path.join(config['CATALOG_ID'])))
        print('logging to file: {}'.format(log_file))
        cmd = 'python3 create_stac_catalog.py --config {} >> {} 2>&1'.format(config_filename, log_file)
        print(cmd)
        os.system(cmd)

