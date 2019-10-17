import os
import shutil
import pathlib
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime
import json
import argparse
import time
import urllib.request
import uuid
import re

import boto3
import rasterio
import rasterio.warp
import shapely.geometry
import satstac


STAC_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

def generic_image_id_function(input_key):
    # In this example,
    # the s3 key 'output/060901NE_COG.TIF'
    # becomes
    # an item id '060901NE'
    image_id = os.path.basename(input_key).split('_')[0]
    return image_id


def naip_image_id_function(input_key):
    # In this example,
    # the s3 key 'wi/2017/100cm/rgb/42087/m_4208717_nw_16_1_20170922.tif'
    # becomes
    # an item id '42087/m_4208717_nw_16_1_20170922'
    subdir = input_key.split('/')[-2]
    image_id = '{}/{}'.format(subdir, os.path.basename(input_key).split('.')[0])
    return image_id


def naip_fgdc_function(input_key):
    return input_key.replace('/rgb/', '/fgdc/').replace('.tif', '.txt')


# map config strings to python functions (originally had config as a python
# dict so you could directly pass functions; but that doesn't work with json-based
# config so here we are)
config_to_function_map = {
            'NAIP_FGDC_FUNCTION': naip_fgdc_function,
            'NAIP_IMAGE_ID_FUNCTION': naip_image_id_function,
            'GENERIC_IMAGE_ID_FUNCTION': generic_image_id_function,
        }


def create_item(stac_config, image_id, image_url, bounds, epsg):
    item = {
             'id': image_id,
             'type': 'Feature',
             # have [ left, bottom, right, top ]
             # want [ lower left lon, lower left lat, upper right lon, upper right lat ]
             # which is the same thing.
             'bbox': list(bounds),
             'geometry': shapely.geometry.mapping(shapely.geometry.box(*bounds)),
             'properties': {
                 'datetime': stac_config.get('ITEM_TIMESTAMP', None),
                 'collection': stac_config.get('ITEM_COLLECTION_PROPERTY', stac_config['COLLECTION_METADATA']['id']),
                 'eo:epsg': epsg,
             },
             'assets': {
                 # following example at
                 # https://storage.googleapis.com/pdd-stac/disasters/hurricane-harvey/0831/20170831_172754_101c.json
                 # to get it to show up in stac-browser
                 'visual': {
                     'href': image_url,
                     'type': 'image/vnd.stac.geotiff; cloud-optimized=true',
                 }
             }
           }

    # naip-specific hack to add footprint id property
    if 'naip-visualization' in image_url:
        # turns '40077/m_4007746_ne_18_1_20170709' into '4007746_ne'
        try:
            parts = image_id.split('/')[1].split('_')
            item['properties']['naip:quarter_quad_id'] = '{}_{}'.format(parts[1], parts[2])
        except:
            print('failed to turn {} into naip quarter quad id'.format(image_id))

    return item


def publish_to_s3(stac_config, catalog_dir):
    s3 = boto3.resource('s3', region_name=stac_config['OUTPUT_BUCKET_REGION'])

    # simply pushes every json file in catalog_dir to S3, maintining dir hierarchy
    root_path = pathlib.Path(catalog_dir)
    json_paths = list(root_path.rglob("*.json"))
    for json_path in json_paths:
        # remove the temp_dir part of the path to get s3 key
        s3_key = str(json_path.relative_to(root_path))

        new_object = s3.Object(stac_config['OUTPUT_BUCKET_NAME'], s3_key)
        print('uploading {} to {}'.format(str(json_path), s3_key))
        new_object.upload_file(str(json_path))
        print('...upload complete')


def get_s3_listing(stac_config):
    # NOTE, we can't just use "bucket.objects.all()" b/c then there's no
    #       way to support requester pays (as far as I can tell)
    # Could just set "MaxKeys" but that doesn't actually guarantee you'll get that many
    bucket_prefix = stac_config.get('BUCKET_PREFIX', None)
    response = boto3.client('s3').list_objects(Bucket=stac_config['BUCKET_NAME'],
           RequestPayer='requester',
           Prefix=bucket_prefix,
           )
    all_objects = response['Contents']
    while response['IsTruncated']:
        # per doc, use Marker="last Key" instead of Marker=NextMarker since
        # we don't set a Delimiter argument.
        assert 'NextMarker' not in response
        marker = response['Contents'][-1]['Key']
        print('making request for next page with marker {}'.format(marker))
        response = boto3.client('s3').list_objects(Bucket=stac_config['BUCKET_NAME'],
               RequestPayer='requester',
               Prefix=bucket_prefix,
               Marker=marker,
               )
        all_objects += response['Contents']
    all_keys = [o['Key'] for o in all_objects]
    return all_keys


def download_s3_file(bucket, key, filename, requester_pays=False):
    s3_url = 's3://{}/{}'.format(bucket, key)

    print("Downloading {} to {}".format(s3_url, filename))
    if requester_pays:
        bucket.download_file(key, filename, {'RequestPayer': 'requester'})
    else:
        bucket.download_file(key, filename)
    print('...S3 download complete')


def update_spatial_extent(extent, new_bounds):
    left, bottom, right, top = new_bounds
    if left < extent['left']:
        extent['left'] = left
    if right > extent['right']:
        extent['right'] = right
    if bottom < extent['bottom']:
        extent['bottom'] = bottom
    if top > extent['top']:
        extent['top'] = top


def update_temporal_extent(extent, datetime_obj):
    try:
        # convert if needed
        datetime_obj = datetime.strptime(datetime_obj, STAC_DATE_FORMAT)
    except:
        # already datetime
        pass

    if (extent['earliest'] is None) or (datetime_obj < extent['earliest']):
        extent['earliest'] = datetime_obj
    if (extent['latest'] is None) or (datetime_obj > extent['latest']):
        extent['latest'] = datetime_obj


def add_fgdc_metadata_to_item(stac_config, temp_dir, bucket, input_key, item_dict):
    fgdc_key = config_to_function_map[stac_config['S3_KEY_TO_FGDC_S3_KEY']](input_key)
    fgdc_file = os.path.join(temp_dir, os.path.basename(fgdc_key))

    t0 = time.time()
    download_s3_file(bucket, fgdc_key, fgdc_file, requester_pays=True)
    print('time to download fgdc file: {}s'.format(time.time()-t0))

    fgdc_file_xml = fgdc_file.replace('.txt', '.xml')

    print('converting {} to {} using mp tool'.format(fgdc_file, fgdc_file_xml))
    args = ['mp', '-x', fgdc_file_xml, fgdc_file]
    t0 = time.time()
    output = subprocess.check_output(args).decode()
    print('time to run mp: {}s'.format(time.time()-t0))
    print(output)

    t0 = time.time()
    tree = ET.parse(fgdc_file_xml)
    root = tree.getroot()

    # NOTE these are presumably naip specific, will need to reconsider
    #      if ever supporting another source with fgdc metadata

    # time period calendar date
    for e in root.findall('./idinfo/timeperd/timeinfo/sngdate/caldate'):
        item_dict['properties']['datetime'] = datetime.strptime(e.text, '%Y%m%d')
        print('parsed date: {} from raw: {}'.format(item_dict['properties']['datetime'], e.text))

    # place keywords
    places = [e.text for e in root.findall('./idinfo/keywords/place/placekey')]
    # Can't find a stac standard property for this but it seems useful
    item_dict['properties']['place_keywords'] = ';'.join(places)
    print(item_dict['properties']['place_keywords'])
    print('time to parse xml: {}s'.format(time.time()-t0))

    # do some more naip-specific stuff since this part would need to be reworked 
    # to support non-naip sources with fgdc metadata anyway
    item_dict['properties']['eo:gsd'] = 1.0
    item_dict['properties']['eo:instrument'] = "Leica ADS100"
    item_dict['properties']['eo:constellation'] = "NAIP"
    item_dict['properties']['eo:bands'] = [
            # credit Jeff Albrecht (github.com/geospatial-jeff):
            # https://github.com/geospatial-jeff/cognition-datasources-naip/blob/master/docs/example.json
            {
                "name": "B01",
                "common_name": "red",
                "gsd": 1.0,
                "center_wavelength": 635,
                "full_width_half_max": 16,
                "accuracy": 6
            },
            {
                "name": "B02",
                "common_name": "green",
                "gsd": 1.0,
                "center_wavelength": 555,
                "full_width_half_max": 30,
                "accuracy": 6
            },
            {
                "name": "B03",
                "common_name": "blue",
                "gsd": 1.0,
                "center_wavelength": 465,
                "full_width_half_max": 30,
                "accuracy": 6
            },
            {
                "name": "B04",
                "common_name": "nir",
                "gsd": 1.0,
                "center_wavelength": 845,
                "full_width_half_max": 37,
                "accuracy": 6
            }
        ]

    # add link to fgdc metadata
    item_dict['assets']['metadata'] = {
                'href': 's3://{}/{}'.format(stac_config['BUCKET_NAME'], fgdc_key),
                'type': 'text/plain',
                'title': 'FGDC metadata',
            }


def assert_valid_stac_lint(stac_validator_output_string):
    # Note, currently sat-stac names the collection file "catalog.json" with
    # no way to override (see https://github.com/sat-utils/sat-stac/issues/36)
    # This causes stac-validator to get confused and think the collection
    # is a catalog (see https://github.com/sparkgeo/stac-validator/blob/v0.1.3/stac_validator.py#L238)
    # Once sat-stac issue 36 is fixed, we can rename the collection file so it
    # will get detected as a collection due to presence of license, stac_version, etc. keys
    # (see https://github.com/sparkgeo/stac-validator/blob/v0.1.3/stac_validator.py#L206)
    output = json.loads(stac_validator_output_string)
    for result in output:
        if not result['valid_stac']:
            raise Exception('STAC entity {} did not validate: {}'.format(
                result['path'], result['error_message']))


def lint_stac_local(stac_config, catalog_dir):
    # validate all catalog/collection/items with stac_validator
    # (here we can't just use --follow since we haven't uploaded
    # to s3 yet so links don't make sense)
    root_path = pathlib.Path(catalog_dir)
    json_paths = [str(a) for a in root_path.rglob("*.json")]
    for json_path in json_paths:
        cmd = ['stac_validator', json_path, '-v', 'v' + stac_config['COLLECTION_METADATA']['stac_version'],
                '--verbose']
        print(' '.join(cmd))
        output = subprocess.check_output(cmd).decode()
        print(output)
        assert_valid_stac_lint(output)


def lint_uploaded_stac(stac_config, root_catalog_url):
    # here we can use --follow to do everything at once
    cmd = ['stac_validator', root_catalog_url, '-v', 'v' + stac_config['COLLECTION_METADATA']['stac_version'],
            '--follow', '--verbose']
    print(' '.join(cmd))
    output = subprocess.check_output(cmd).decode()
    print(output)
    assert_valid_stac_lint(output)


def validate_cog(url):
    from .validate_cloud_optimized_geotiff import validate, ValidateCloudOptimizedGeoTIFFException
    # TODO what if if requester pays?
    vsicurl_url = url.replace('http://', '/vsicurl/').replace('https://', '/vsicurl/')

    print('checking if valid COG: {}'.format(vsicurl_url))
    t0 = time.time()
    try:
        warnings, errors, details = validate(vsicurl_url)
    except ValidateCloudOptimizedGeoTIFFException as e:
        # this exception gets thrown in case of e.g. a jp2 file that we can still convert
        print(str(e))
        print('got ValidateCloudOptimizedGeoTIFFException exception; assuming file needs conversion')
        return False

    # XXX argh this is way too slow, like 3sec per tif, at least from laptop
    print('time to run validate_cloud_optimized_geotiff: {}sec'.format(time.time()-t0))

    if warnings:
        print('The following warnings were found:')
        for warning in warnings:
            print(' - ' + warning)
    if errors:
        print('{} is NOT a valid cloud optimized GeoTIFF.'.format(vsicurl_url))
        print('The following errors were found:')
        for error in errors:
            print(' - ' + error)
        print('')
        return False

    print('{} is a valid COG'.format(vsicurl_url))
    return True


def convert_to_cog(stac_config, temp_dir, input_url):
    parts = os.path.basename(input_url).split('.')
    cog_filename = ''.join(parts[:-1]) + '_COG.TIF'
    # TODO think about this more
    s3_key = '{}/{}'.format(stac_config['COLLECTION_METADATA']['id'], cog_filename)
    cog_url = 's3://{}/{}'.format(stac_config['OUTPUT_BUCKET_NAME'], s3_key)

    # copy file to local
    input_filename = os.path.join(temp_dir, os.path.basename(input_url))
    print('downloading file {}'.format(input_filename))
    cmd = ['aws', 's3', 'cp', input_url, input_filename]
    print(' '.join(cmd))
    try:
        output = subprocess.check_output(cmd).decode()
        print(output)
    except:
        print('retrying {} with urlretrieve'.format(input_url))
        urllib.request.urlretrieve(input_url, input_filename)
    print('...download complete')

    output_filename = os.path.join(temp_dir, cog_filename)
    args = ['rio', 'cogeo', 'create', input_filename, output_filename, '--cog-profile', 'deflate']
    print(' '.join(args))
    output = subprocess.check_output(args).decode()
    print(output)

    # upload COG to s3
    print('uploading {} to {}'.format(output_filename, s3_key))
    cmd = ['aws', 's3', 'cp', output_filename, cog_url]
    print(' '.join(cmd))
    output = subprocess.check_output(cmd).decode()
    print(output)
    print('...upload complete')

    return cog_url


def build_https_url_from_bucket_name(bucket_name, bucket_region):
    return 'https://s3.{bucket_region}.amazonaws.com/{bucket_name}/'.format(
                bucket_region=bucket_region,
                bucket_name=bucket_name,
            )


def validate_stac_config(stac_config):
    """ Place to make sure optional params are set to reasonable defaults,
        optional id's are generated automatically, bail if required params
        aren't present, etc.
    """
    # generate catalog/collection id's if not present
    if not stac_config.get('CATALOG_ID', None):
        stac_config['CATALOG_ID'] = str(uuid.uuid4())

    if not stac_config['COLLECTION_METADATA'].get('id', None):
        # NOTE this is used as part of s3 key path due to sat-stac implementation
        stac_config['COLLECTION_METADATA']['id'] = str(uuid.uuid4())

    if not stac_config.get('BUCKET_BASE_URL', None):
        stac_config['BUCKET_BASE_URL'] = build_https_url_from_bucket_name(
                stac_config['BUCKET_NAME'],
                stac_config['BUCKET_REGION']
            )

    if not stac_config.get('OUTPUT_BUCKET_BASE_URL', None):
        stac_config['OUTPUT_BUCKET_BASE_URL'] = build_https_url_from_bucket_name(
                stac_config['OUTPUT_BUCKET_NAME'],
                stac_config['OUTPUT_BUCKET_REGION']
            )

    if not stac_config.get('S3_KEY_TO_IMAGE_ID', None):
        stac_config['S3_KEY_TO_IMAGE_ID'] = 'GENERIC_IMAGE_ID_FUNCTION'


def get_initial_spatial_extent(collection):
    try:
        initial = collection.data['extent']['spatial']
        print('returning existing initial spatial extent')
        extent = {
                    'left': initial[0],
                    'bottom': initial[1],
                    'right': initial[2],
                    'top': initial[3],
                }
    except:
        print('returning empty initial spatial extent')
        extent = {
                    'left': float('inf'),
                    'bottom': float('inf'),
                    'right': float('-inf'),
                    'top': float('-inf'),
                }
    print('initial spatial extent: {}'.format(extent))
    return extent


def get_initial_temporal_extent(collection):
    try:
        initial = collection.data['extent']['temporal']
        print('returning existing initial temporal extent')
        extent = {
                    'earliest': datetime.strptime(initial[0], STAC_DATE_FORMAT),
                    'latest': datetime.strptime(initial[1], STAC_DATE_FORMAT),
                }
    except:
        print('returning empty initial temporal extent')
        extent = {
                    'earliest': None,
                    'latest': None,
                }
    print('initial temporal extent: {}'.format(extent))
    return extent


def add_footprint_id_to_item(stac_config, input_key, item_dict):
    if 'FILENAME_REGEX' not in stac_config:
        return

    regex = stac_config['FILENAME_REGEX']
    filename = os.path.basename(input_key)
    m = re.search(regex, filename)

    if m is None:
        print('regex {} did not match {}'.format(regex, filename))
        return

    try:
        footprint_id = m.group('footprint')
    except IndexError:
        print('regex {} matched {} but did not a "footprint" group'.format(regex, filename))
        return

    print('extracted footprint id from filename: {}'.format(footprint_id))
    item_dict['properties']['footprint_id'] = footprint_id


def create_stac_catalog(temp_dir, stac_config):
    if os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    validate_stac_config(stac_config)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(stac_config['BUCKET_NAME'])

    input_keys = get_s3_listing(stac_config)
    input_keys = [f for f in input_keys if f.endswith(stac_config['COG_SUFFIX'])]

    item_dicts = []

    collection_dir = os.path.join(stac_config['OUTPUT_BUCKET_BASE_URL'], stac_config['ROOT_CATALOG_DIR'], stac_config['COLLECTION_METADATA']['id'])
    collection_url = os.path.join(collection_dir, 'catalog.json')
    collection_already_exists = False
    try:
        collection = satstac.Collection.open(collection_url)
        print('successfully opened existing collection at {}'.format(collection_url))
        collection_already_exists = True
    except:
        print('creating new collection')
        collection = satstac.Collection(stac_config['COLLECTION_METADATA'])

    spatial_extent = get_initial_spatial_extent(collection)
    temporal_extent = get_initial_temporal_extent(collection)

    for input_key in input_keys:

        if stac_config.get('REQUESTER_PAYS', False):
            # rasterio needs s3 url not http for requester pays
            # NOTE this will also result in the stac item asset link looking like
            #      s3://... instead of https:// which is actually good b/c the ONLY
            #      way to access it is via S3 api with requester pays
            image_url = 's3://{}/{}'.format(stac_config['BUCKET_NAME'], input_key)
        else:
            image_url = stac_config['BUCKET_BASE_URL'] + input_key

        is_valid_cog = validate_cog(image_url)
        if not is_valid_cog and stac_config.get('ALLOW_COG_CONVERSION', False):
            cog_image_url = convert_to_cog(stac_config, temp_dir, image_url)

            # create the stac item pointing to the COG
            # TODO also reference the original file in assets?
            image_url = cog_image_url
        elif not is_valid_cog:
            print('{} is invalid COG but automatic COG conversion disabled; enable with ALLOW_COG_CONVERSION'.format(image_url))
        elif is_valid_cog:
            print('{} is a valid COG'.format(image_url))

        with rasterio.open(image_url, 'r') as raster_file:
            bounds = raster_file.bounds
            bounds_geo = rasterio.warp.transform_bounds(raster_file.crs, 4326,
                    bounds.left, bounds.bottom, bounds.right, bounds.top)
            print(bounds_geo)

            image_id = config_to_function_map[stac_config['S3_KEY_TO_IMAGE_ID']](input_key)
            print('determined item id {} from s3 key {}'.format(image_id, input_key))

            epsg = None
            if raster_file.crs.is_epsg_code:
                epsg = raster_file.crs.to_epsg()

            item_dict = create_item(stac_config, image_id, image_url, bounds_geo, epsg)
            add_footprint_id_to_item(stac_config, input_key, item_dict)
            item_dicts.append(item_dict)

        if 'S3_KEY_TO_FGDC_S3_KEY' in stac_config:
            add_fgdc_metadata_to_item(stac_config, temp_dir, bucket, input_key, item_dict)

        update_spatial_extent(spatial_extent, bounds_geo)
        update_temporal_extent(temporal_extent, item_dict['properties']['datetime'])

        # convert item datetime obj to string (if needed)
        try:
            item_dict['properties']['datetime'] = item_dict['properties']['datetime'].strftime(STAC_DATE_FORMAT)
        except AttributeError:
            # This is an expected failure if datetime is already a string from config
            pass

    # update collection metadata with max bounds discovered from all rasters
    if not stac_config['COLLECTION_METADATA'].get('extent', None):
        stac_config['COLLECTION_METADATA']['extent'] = {}

    stac_config['COLLECTION_METADATA']['extent']['spatial'] = [
                spatial_extent['left'],
                spatial_extent['bottom'],
                spatial_extent['right'],
                spatial_extent['top'],
            ]
    print('determined spatial extent: {}'.format(stac_config['COLLECTION_METADATA']['extent']['spatial']))

    stac_config['COLLECTION_METADATA']['extent']['temporal'] = [
                temporal_extent['earliest'].strftime(STAC_DATE_FORMAT),
                temporal_extent['latest'].strftime(STAC_DATE_FORMAT),
            ]
    print('determined temporal extent: {}'.format(stac_config['COLLECTION_METADATA']['extent']['temporal']))

    collection.data['extent']['spatial'] = stac_config['COLLECTION_METADATA']['extent']['spatial']
    collection.data['extent']['temporal'] = stac_config['COLLECTION_METADATA']['extent']['temporal']

    if collection_already_exists:
        # so that changes to collection are written to disk instead of trying
        # to write to remote url...
        collection.save_as(os.path.join(temp_dir, stac_config['ROOT_CATALOG_DIR'], stac_config['COLLECTION_METADATA']['id'], 'catalog.json'))

    # try to open existing root catalog so we can append to it,
    # otherwise create a new one
    root_catalog_dir = os.path.join(stac_config['OUTPUT_BUCKET_BASE_URL'], stac_config['ROOT_CATALOG_DIR'])
    root_catalog_url = os.path.join(root_catalog_dir, 'catalog.json')
    try:
        catalog = satstac.Catalog.open(root_catalog_url)
        print('successfully opened existing root catalog at {}'.format(root_catalog_url))
    # Can't just catch STACError b/c satstac tries to make a signed s3 url if the original
    # url fails, but parses the URL wrong which ultimately causes an SSLError
    # except satstac.thing.STACError:
    except:
        print('creating new root catalog')
        catalog = satstac.Catalog.create(
                  id=stac_config['CATALOG_ID'],
                  description=stac_config['CATALOG_DESCRIPTION'],
                  root=root_catalog_dir,
                )

    catalog.save_as(os.path.join(temp_dir, stac_config['ROOT_CATALOG_DIR'], 'catalog.json'))

    if not collection_already_exists:
        # only if collection not already linked to root catalog; otherwise
        # the collections initial list of links is cleared out
        catalog.add_catalog(collection)

    for item_dict in item_dicts:
        collection.add_item(satstac.Item(item_dict))

    if not stac_config.get('DISABLE_STAC_LINT', False):
        lint_stac_local(stac_config, temp_dir)

    # Comment this out if testing to avoid actually pushing catalog to S3
    publish_to_s3(stac_config, temp_dir)

    if not stac_config.get('DISABLE_STAC_LINT', False):
        lint_uploaded_stac(stac_config, root_catalog_url)

    # return final params so library user can update db, etc.
    return stac_config


def parse_args_and_run():
    default_config_path = "config.json"
    default_temp_dir = '/work'

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", 
            help="Path to json config file (default: {})".format(default_config_path), 
                default=default_config_path,
            )
    parser.add_argument("-d", "--tempdir", 
            help="Path to temp working dir (default: {})".format(default_temp_dir), 
                default=default_temp_dir,
            )
    args = parser.parse_args()
    config_path = args.config
    with open(config_path) as f:
        stac_config = json.loads(f.read())
    temp_dir = args.tempdir
    create_stac_catalog(temp_dir, stac_config)


if __name__ == '__main__':
    parse_args_and_run()
