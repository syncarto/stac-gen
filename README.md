`stac-gen` is a tool for creating a static [STAC](https://github.com/radiantearth/stac-spec) catalog using [sat-stac](https://github.com/sat-utils/sat-stac) and uploading the catalog to an S3 bucket. It is configured via a json file (documented below). Once a bucket is configured, the tool will determine the spatial extent of each raster in the bucket, create corresponding STAC item json files, and also create root catalog and corresponding collection json files. Once this is completed, the json files will be uploaded to the configured output bucket (can be the same or different as input bucket), at which point the catalog is suitable for indexing via e.g. [sat-api](https://github.com/sat-utils/sat-api).

The demo use case is to create a STAC catalog for the [AWS publicly available NAIP dataset](https://registry.opendata.aws/naip/) in the `naip-visualization` bucket. The tool will create one STAC collection per state per year, and one STAC item per geotiff in the NAIP bucket. Each collection is linked to a root STAC catalog.

To understand the output of this tool, look at the contents of the `example_output` subdirectory. This is a small subset of the STAC catalog that is generated for NAIP, including the root catalog, one collection, and a handful of items belonging to that collection.

There is also an example configuration for creating a STAC catalog for a "generic" bucket. The script will discover every TIF in the bucket and create corresponding STAC items for each one.

The resulting STAC catalog can optionally be linted using using [stac-validator](https://github.com/sparkgeo/stac-validator) (see the `DISABLE_STAC_LINT` configuration option).

# Command line usage

```bash
$ python3 stac_gen/create_stac_catalog.py -h
usage: create_stac_catalog.py [-h] [-c CONFIG]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Path to json config file (default: config.json)
```

# Running in Docker

It is suggested to run this tool via Docker using the provided `Dockerfile`.

```bash
# build docker image
docker build . -t stac-gen

# Note, AWS_REQUEST_PAYER only needed for NAIP bucket.
# Mount your AWS credentials file as needed here.
docker run --rm -it --name stac-gen-1 \
  -v $HOME/.aws/credentials:/root/.aws/credentials \
  -v $PWD:/code \
  -e AWS_CREDENTIAL_FILE=/root/.aws/credentials \
  -e AWS_PROFILE=default \
  -e AWS_REQUEST_PAYER=requester \
  stac-gen \
  python3 stac_gen/create_stac_catalog.py --config naip_config.json
```
Alternatively, you can build and create the container with the docker-compose command: 

``` 
docker-compose up
```
-- Then the container can be accessed with:
```
$ docker exec -it stac-gen-1 /bin/bash
```

# Config file details

```javascript
{
  // The details of the "input" bucket which has the images the STAC catalog will point at:
  "BUCKET_NAME": "naip-visualization",
  "BUCKET_REGION": "us-west-2",
  "BUCKET_BASE_URL": "https://s3.us-west-2.amazonaws.com/naip-visualization/",
  // Optional; limit to files within this prefix:
  "BUCKET_PREFIX": "wi/2017/100cm/rgb/",
  // Is it a requester pays bucket? (True for NAIP, likely false for others; if true, asset links will be s3:// instead of https://):
  "REQUESTER_PAYS": true,
  // Details of the "output" bucket where the STAC catalog will be uploaded to.
  // (Can be same as the input bucket if desired):
  "OUTPUT_BUCKET_NAME": "your-bucket-name",
  "OUTPUT_BUCKET_REGION": "us-west-2",
  "OUTPUT_BUCKET_BASE_URL": "https://s3.us-west-2.amazonaws.com/your-bucket-name/",
  // Only files with this suffix will be indexed:
  "COG_SUFFIX": ".tif",
  // Choose ID and description for your catalog as desired:
  "CATALOG_ID": "NAIP_wi_2017_100cm",
  "CATALOG_DESCRIPTION": "NAIP_wi_2017_100cm",
  // The root catalog will be uploaded under this S3 prefix:
  "CATALOG_ROOT_DIR": "naip",
  // Add metadata as desired for the collection that will be linked to the root catalog:
  "COLLECTION_METADATA": {
    // Note due to behavior of sat-stac, the "id" will also be the location of the collection json file on S3:
    "id": "wi/2017/100cm/rgb",
    "description": "NAIP wi 2017 100cm",
    "title": "NAIP wi 2017 100cm",
    "license": "Public Domain with Attribution",
    "stac_version": "0.6.0",
    // Can leave this empty; spatial and temporal collection extent will be discovered and filled in
    "extent": {}
  },
  // For NAIP this is not needed; but otherwise you need to provide a timestamp to be used for all items:
  // (TODO: make this configurable per-item)
  "ITEM_TIMESTAMP": "2010-01-01T00:00:00Z",
  // Set this to "NAIP_IMAGE_ID_FUNCTION" for NAIP, or "GENERIC_IMAGE_ID_FUNCTION" otherwise
  // See create_stac_catalog.py for details of what this does
  "S3_KEY_TO_IMAGE_ID": "NAIP_IMAGE_ID_FUNCTION",
  // Optional; set this as follows for NAIP or leave out otherwise:
  "S3_KEY_TO_FGDC_S3_KEY": "NAIP_FGDC_FUNCTION",
  // Set this to true if you want to disable STAC linting after catalog creation (faster for large datasets)
  "DISABLE_STAC_LINT": false,
}
```

# NAIP details

Start from `naip_config.json` for a demo of how to create and upload a STAC catalog for the NAIP Wisconsin 2017 dataset.

You can also use the `run_all_naip.py` wrapper script to loop through all available NAIP states and years.

Additionally (for NAIP only), the corresponding FGDC metadata file will be downloaded and converted to XML using the [USGS mp tool](https://geology.usgs.gov/tools/metadata/tools/doc/mp.html). Currently only the image acquisition date and place keywords are pulled out of the FGDC metadata and added to the STAC item, but this could be expanded in the future.

# "Generic imagery bucket" details

Start from `example_config.json` if you want to run the tool on all TIF files stored in the configured bucket.
