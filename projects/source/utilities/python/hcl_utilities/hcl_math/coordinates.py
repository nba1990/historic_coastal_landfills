# Utility methods and functions to convert coordinates (easting, northing) to (latitude, longitude) and vice versa.
# Concept: Projection mapping
# Project: Historic Coastal Landfills
# Organisation: A Future Without Rubbish CIC, UK
# Author: Bharadwaj Raman
# Date First Authored: 27/01/2023

# Import modules
import pyproj


def convert_easting_northing_to_latitude_longitude(easting, northing):
    """Convert easting and northing to latitude and longitude Coordinates."""
    # Define Transformer for the projection mapping from easting and northing to latitude and longitude
    # Coordinate Reference System Codes can be found here: https://epsg.io
    transformer = pyproj.transformer.Transformer.from_crs("EPSG:27700", "EPSG:4326")

    # Conversion for pyproj_v1 (older method)
    # # Define the projection for the easting and northing coordinates
    # easting_northing_proj = pyproj.Proj(init='epsg:27700')
    #
    # # Define the projection for the latitude and longitude coordinates
    # latitude_longitude_proj = pyproj.Proj(init='epsg:4326')

    # convert the Coordinates
    # longitude, latitude = pyproj.transform(easting_northing_proj, latitude_longitude_proj, easting, northing)

    # Conversion for pyproj_v2 (newer method)
    latitude, longitude = transformer.transform(easting, northing)

    return latitude, longitude


def convert_latitude_longitude_to_easting_northing(latitude, longitude):
    """Convert latitude and longitude to easting and northing Coordinates."""
    # Define Transformer for the projection mapping from latitude and longitude to easting and northing
    # Coordinate Reference System Codes can be found here: https://epsg.io
    transformer = pyproj.transformer.Transformer.from_crs("EPSG:4326", "EPSG:27700")

    # Conversion for pyproj_v1 (older method)
    # # Define the projection for the easting and northing coordinates
    # easting_northing_proj = pyproj.Proj(init='epsg:27700')
    #
    # # Define the projection for the latitude and longitude coordinates
    # latitude_longitude_proj = pyproj.Proj(init='epsg:4326')

    # convert the Coordinates
    # longitude, latitude = pyproj.transform(easting_northing_proj, latitude_longitude_proj, easting, northing)

    # Conversion for pyproj_v2 (newer method)
    easting, northing = transformer.transform(latitude, longitude)

    return round(easting), round(northing)
