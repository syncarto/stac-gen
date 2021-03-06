import os
import sys
import argparse
import logging

from .catalog import Catalog
from .version import __version__


logger = logging.getLogger(__name__)


def parse_args(args):
    desc = 'stac (v%s)' % __version__
    dhf = argparse.ArgumentDefaultsHelpFormatter
    parser0 = argparse.ArgumentParser(description=desc)

    pparser = argparse.ArgumentParser(add_help=False)
    pparser.add_argument('--version', help='Print version and exit', action='version', version=__version__)
    pparser.add_argument('--log', default=2, type=int,
                         help='0:all, 1:debug, 2:info, 3:warning, 4:error, 5:critical')

    # add subcommands
    subparsers = parser0.add_subparsers(dest='command')

    # command 1
    parser = subparsers.add_parser('create', parents=[pparser], help='Create a root catalog', formatter_class=dhf)
    parser.add_argument('id', help='ID of the new catalog')
    parser.add_argument('description', help='Description of new catalog')
    parser.add_argument('--filename', help='Filename of catalog', default='catalog.json')
    group = parser.add_argument_group('root catalog options (mutually exclusive)')
    group = group.add_mutually_exclusive_group(required=True)
    group.add_argument('--root', help='Filename to existing root catalog', default=None)
    group.add_argument('--endpoint', help='Endpoint for this new root catalog', default=None)

    # command 2
    h = 'Update entire catalog with a new endpoint (update self links)'
    parser = subparsers.add_parser('publish', parents=[pparser], help=h, formatter_class=dhf)
    parser.add_argument('root', help='Filename to existing root catalog')
    parser.add_argument('endpoint', help='New endpoint')
    # parser.add_argument()

    # turn Namespace into dictinary
    parsed_args = vars(parser0.parse_args(args))

    return parsed_args


def cli():
    args = parse_args(sys.argv[1:])
    logger.setLevel(args.pop('log') * 10)
    cmd = args.pop('command')

    if cmd == 'create':
        if args['root'] is not None:
            root = Catalog.open(args['root'])
            cat = Catalog.create(id=args['id'], description=args['description'])
            root.add_catalog(cat)
        else:
            cat = Catalog.create(id=args['id'], description=args['description'], root=args['endpoint'])
            cat.save_as(args['filename'])
    elif cmd == 'publish':
        cat = Catalog.open(args['root'])
        cat.publish(args['endpoint'])


if __name__ == "__main__":
    cli()
