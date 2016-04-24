# City Extracts
Extract city statistics via web APIs

We have following data sources:
1. OpenStreeMap city data from [metro extracts](https://mapzen.com/data/metro-extracts/).
2. Factual API: http://developer.factual.com/
3. AngelList API: https://angel.co/api


## System Requirement
Python 3 is required. Addtional Python packages can be found in `requirements.txt`.
To run the code, the machine need to have [PostGIS](http://postgis.net/) and [`osm2pgsql`](http://wiki.openstreetmap.org/wiki/Osm2pgsql).
The code is only tested on Linux, i.e. `wget` and `sudo` are used to download OSM data and create postgres database.

## Configuration
Configurations are in `extract.ini`. Factual and AngelList API keys are needed.
