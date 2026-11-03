# CARTO's reference GeoTIFF -> RaQuet converter. It needs the GDAL Python
# bindings, which have no PyPI wheels; this image's NumPy 1.x caps pyarrow at 19.
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.11.4
RUN apt-get -qq update && apt-get -qq install -y --no-install-recommends python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && pip install -q --break-system-packages raquet-io==0.9.0 pyarrow==19.0.1
