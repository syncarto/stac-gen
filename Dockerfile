FROM ubuntu:18.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
   python3 \
   python3-dev \
   python3-pip \
   gdal-bin \
   libgdal-dev \
   libgdal20 \
   python3-gdal \
   wget

ADD . /code

# Install python pip dependencies.
RUN pip3 install -r /code/requirements.txt

# For some reason rio cli wants these set
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# fix for https error from rasterio trying to get geotiff headers
# https://stackoverflow.com/questions/3160909/how-do-i-deal-with-certificates-using-curl-while-trying-to-access-an-https-url
RUN mkdir -p /etc/pki/tls/certs/
RUN cp /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt

# download "mp" tool for fgdc metadata->xml conversion
WORKDIR /fgdc
RUN wget https://geology.usgs.gov/tools/metadata/bin_lnx.tar.gz
RUN tar xzvf bin_lnx.tar.gz
RUN cp tools/bin/mp.lnx /usr/local/bin/mp

WORKDIR /code

CMD ["./run_naip_rancher.sh"]
