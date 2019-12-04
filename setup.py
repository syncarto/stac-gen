from setuptools import setup

setup(name='stac-gen',
      version='0.1',
      description='Python tool for creating a STAC catalog for NAIP and other imagery sources',
      url='https://github.com/syncarto/stac-gen',
      author='Michael Hiley',
      author_email='hiley@syncarto.com',
      license='MIT',
      packages=['stac_gen', 'stac_gen.satstac'],
      install_requires=[
                'boto3',
                'rasterio',
                'shapely',
                'stac-validator',
                # sat-stac dependencies
                'requests>=2.19.1',
                'python-dateutil~=2.7.5',
          ],
      )
