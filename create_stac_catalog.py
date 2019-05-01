import os
import shutil
import pathlib
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime
import json
import argparse

import boto3
import rasterio
import rasterio.warp
import shapely.geometry
import satstac


# temp directory inside container; can leave this as-is
TEMP_DIR = '/work'

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


def create_item(image_id, image_url, bounds, epsg):
    item = {
             'id': image_id,
             'type': 'Feature',
             # have [ left, bottom, right, top ]
             # want [ lower left lon, lower left lat, upper right lon, upper right lat ]
             # which is the same thing.
             'bbox': list(bounds),
             'geometry': shapely.geometry.mapping(shapely.geometry.box(*bounds)),
             'properties': {
                 'datetime': STAC_CONFIG.get('ITEM_TIMESTAMP', None),
                 'collection': STAC_CONFIG.get('ITEM_COLLECTION_PROPERTY', STAC_CONFIG['CATALOG_ID']),
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
    return item


def publish_to_s3(catalog_dir):
    s3 = boto3.resource('s3', region_name=STAC_CONFIG['OUTPUT_BUCKET_REGION'])

    # simply pushes every json file in catalog_dir to S3, maintining dir hierarchy
    root_path = pathlib.Path(catalog_dir)
    json_paths = list(root_path.rglob("*.json"))
    for json_path in json_paths:
        # remove the TEMP_DIR part of the path to get s3 key
        s3_key = str(json_path.relative_to(root_path))

        new_object = s3.Object(STAC_CONFIG['OUTPUT_BUCKET_NAME'], s3_key)
        print('uploading {} to {}'.format(str(json_path), s3_key))
        new_object.upload_file(str(json_path))
        print('...upload complete')


def get_s3_listing():
    # NOTE, we can't just use "bucket.objects.all()" b/c then there's no
    #       way to support requester pays (as far as I can tell)
    # Could just set "MaxKeys" but that doesn't actually guarantee you'll get that many
    bucket_prefix = STAC_CONFIG.get('BUCKET_PREFIX', None)
    response = boto3.client('s3').list_objects(Bucket=STAC_CONFIG['BUCKET_NAME'],
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
        response = boto3.client('s3').list_objects(Bucket=STAC_CONFIG['BUCKET_NAME'],
               RequestPayer='requester',
               Prefix=bucket_prefix,
               Marker=marker,
               )
        all_objects += response['Contents']
    all_keys = [o['Key'] for o in all_objects]
    return all_keys


def download_s3_file(bucket, key, filename, requester_pays=False):
    # since we have to support requester pays, easier to just use awscli
    # than lower level boto3 client
    s3_url = 's3://{}/{}'.format(bucket, key)
    cmd = ['aws', 's3', 'cp', s3_url, filename]
    if requester_pays:
        cmd.append('--request-payer')
    print(' '.join(cmd))
    output = subprocess.check_output(cmd).decode()
    print(output)


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
    if (extent['earliest'] is None) or (datetime_obj < extent['earliest']):
        extent['earliest'] = datetime_obj
    if (extent['latest'] is None) or (datetime_obj > extent['latest']):
        extent['latest'] = datetime_obj


def add_fgdc_metadata_to_item(input_key, item_dict):
    fgdc_key = config_to_function_map[STAC_CONFIG['S3_KEY_TO_FGDC_S3_KEY']](input_key)
    fgdc_file = os.path.join(TEMP_DIR, os.path.basename(fgdc_key))

    download_s3_file(STAC_CONFIG['BUCKET_NAME'], fgdc_key, fgdc_file, requester_pays=True)

    fgdc_file_xml = fgdc_file.replace('.txt', '.xml')

    print('converting {} to {} using mp tool'.format(fgdc_file, fgdc_file_xml))
    args = ['mp', '-x', fgdc_file_xml, fgdc_file]
    output = subprocess.check_output(args).decode()
    print(output)

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

    # do some more naip-specific stuff since this part would need to be reworked 
    # to support non-naip sources with fgdc metadata anyway
    item_dict['properties']['eo:gsd'] = 1.0
    item_dict['properties']['eo:instrument'] = "Leica ADS100"
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
                'href': 's3://{}/{}'.format(STAC_CONFIG['BUCKET_NAME'], fgdc_key),
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


def lint_stac_local(catalog_dir):
    # validate all catalog/collection/items with stac_validator
    # (here we can't just use --follow since we haven't uploaded
    # to s3 yet so links don't make sense)
    root_path = pathlib.Path(catalog_dir)
    json_paths = [str(a) for a in root_path.rglob("*.json")]
    for json_path in json_paths:
        cmd = ['stac_validator', json_path, '-v', 'v' + STAC_CONFIG['COLLECTION_METADATA']['stac_version'],
                '--verbose']
        print(' '.join(cmd))
        output = subprocess.check_output(cmd).decode()
        print(output)
        assert_valid_stac_lint(output)


def lint_uploaded_stac(root_catalog_url):
    # here we can use --follow to do everything at once
    cmd = ['stac_validator', root_catalog_url, '-v', 'v' + STAC_CONFIG['COLLECTION_METADATA']['stac_version'],
            '--follow', '--verbose']
    print(' '.join(cmd))
    output = subprocess.check_output(cmd).decode()
    print(output)
    assert_valid_stac_lint(output)


def main():
    if os.path.isdir(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR)

    input_keys = get_s3_listing()
    input_keys = [f for f in input_keys if f.endswith(STAC_CONFIG['COG_SUFFIX'])]

    item_dicts = []
    spatial_extent = {
                'left': float('inf'),
                'bottom': float('inf'),
                'right': float('-inf'),
                'top': float('-inf'),
            }
    temporal_extent = {
                'earliest': None,
                'latest': None,
            }
    for input_key in input_keys:

        if STAC_CONFIG.get('REQUESTER_PAYS', False):
            # rasterio needs s3 url not http for requester pays
            # NOTE this will also result in the stac item asset link looking like
            #      s3://... instead of https:// which is actually good b/c the ONLY
            #      way to access it is via S3 api with requester pays
            image_url = 's3://{}/{}'.format(STAC_CONFIG['BUCKET_NAME'], input_key)
        else:
            image_url = STAC_CONFIG['BUCKET_BASE_URL'] + input_key

        with rasterio.open(image_url, 'r') as raster_file:
            bounds = raster_file.bounds
            bounds_geo = rasterio.warp.transform_bounds(raster_file.crs, 4326,
                    bounds.left, bounds.bottom, bounds.right, bounds.top)
            print(bounds_geo)

            image_id = config_to_function_map[STAC_CONFIG['S3_KEY_TO_IMAGE_ID']](input_key)
            print('determined item id {} from s3 key {}'.format(image_id, input_key))

            epsg = None
            if raster_file.crs.is_epsg_code:
                epsg = raster_file.crs.to_epsg()

            item_dict = create_item(image_id, image_url, bounds_geo, epsg)
            item_dicts.append(item_dict)

        if 'S3_KEY_TO_FGDC_S3_KEY' in STAC_CONFIG:
            add_fgdc_metadata_to_item(input_key, item_dict)

        update_spatial_extent(spatial_extent, bounds_geo)
        update_temporal_extent(temporal_extent, item_dict['properties']['datetime'])

        # convert item datetime obj to string (if needed)
        try:
            item_dict['properties']['datetime'] = item_dict['properties']['datetime'].strftime(STAC_DATE_FORMAT)
        except AttributeError:
            # This is an expected failure if datetime is already a string from config
            pass

    # update collection metadata with max bounds discovered from all rasters
    STAC_CONFIG['COLLECTION_METADATA']['extent']['spatial'] = [
                spatial_extent['left'],
                spatial_extent['bottom'],
                spatial_extent['right'],
                spatial_extent['top'],
            ]
    print('determined spatial extent: {}'.format(STAC_CONFIG['COLLECTION_METADATA']['extent']['spatial']))
    STAC_CONFIG['COLLECTION_METADATA']['extent']['temporal'] = [
                temporal_extent['earliest'].strftime(STAC_DATE_FORMAT),
                temporal_extent['latest'].strftime(STAC_DATE_FORMAT),
            ]
    print('determined temporal extent: {}'.format(STAC_CONFIG['COLLECTION_METADATA']['extent']['temporal']))

    # try to open existing root catalog so we can append to it,
    # otherwise create a new one
    root_catalog_dir = os.path.join(STAC_CONFIG['OUTPUT_BUCKET_BASE_URL'], STAC_CONFIG['ROOT_CATALOG_DIR'])
    root_catalog_url = os.path.join(root_catalog_dir, 'catalog.json')
    try:
        catalog = satstac.Catalog.open(root_catalog_url)
        print('successfully opened existing root catalog at {}'.format(root_catalog_url))
    except satstac.thing.STACError:
        print('creating new root catalog')
        catalog = satstac.Catalog.create(
                  id=STAC_CONFIG['CATALOG_ID'],
                  description=STAC_CONFIG['CATALOG_DESCRIPTION'],
                  root=root_catalog_dir,
                )

    collection = satstac.Collection(STAC_CONFIG['COLLECTION_METADATA'])
    catalog.save_as(os.path.join(TEMP_DIR, STAC_CONFIG['ROOT_CATALOG_DIR'], 'catalog.json'))
    catalog.add_catalog(collection)

    for item_dict in item_dicts:
        collection.add_item(satstac.Item(item_dict))

    if not STAC_CONFIG.get('DISABLE_STAC_LINT', False):
        lint_stac_local(TEMP_DIR)

    # Comment this out if testing to avoid actually pushing catalog to S3
    publish_to_s3(TEMP_DIR)

    if not STAC_CONFIG.get('DISABLE_STAC_LINT', False):
        lint_uploaded_stac(root_catalog_url)


if __name__ == '__main__':
    default_config_path = "config.json"
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", 
            help="Path to json config file (default: {})".format(default_config_path), 
                default=default_config_path,
            )
    args = parser.parse_args()
    config_path = args.config
    with open(config_path) as f:
        STAC_CONFIG = json.loads(f.read())
    main()
