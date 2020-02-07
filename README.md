# Hidden-Gems
A data project focused on finding underrated points of interest in and around some of the world's most famous tourist destinations.

## Datasets
This project utilizes data from Yelp, Tourpedia, and GeoNames to find you the least crowded locations for your next trip.

### Yelp
www.yelp.com/dataset/challenge

This dataset provides reviews and check-ins to classify the well-reviewed spots as well as the most populated businesses at your destination.

### Tourpedia
www.tour-pedia.org/about/datasets.html

Tourpedia gives the geographic locations of tourist spots. These locations may be looked up in the Yelp dataset to distinguish attractions.

### GeoNames
www.geonames.org/

GeoNames gives the geographic locations of points of interest in a number of countries.

## Tech Stack
The data in this project required cleansing and processing in an ETL pipeline. A variety of tools are being considered, however the following are being utilized.

### Spark
The data is being cleansed through a batch process.

## Current Engineering Problems
02/05/2020 - The data from Tourpedia contains errors on numerous rows. Fortunately, the errors are all consistent in that the same fields are swapped and null. The current solution to this problem is just to utilize Spark dataframes to quickly create temporary columns to perform swaps where the errors are present. The fix is performed only on rows with the field `subCategory` is `null`. At the moment, all of the files are affected by these errors, but the data may all be cleansed in the same fashion.

02/07/2020 - The data from Tourpedia includes useful information from countries outside of the US. The Yelp dataset mostly contains data about US businesses. A new dataset, GeoNames, was added to account for points of interest (POIs) in the US.