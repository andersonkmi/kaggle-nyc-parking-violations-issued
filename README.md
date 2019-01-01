# NYC open data traffic violations program

Spark program for analyzing parking violations in NYC using datasets from the 
NYC open data web site.

## Description

This Spark program loads the CSVs from the NYC open data web site and performs
filtering and groupings of traffic violations occurred in New York City.

Some of the data transformations realized are:

- Filter violations by year
- Aggregate (count) violations by year
- Aggregate (count) violations by plate type
- Aggregate (count) violations by registration state

In this program one structure used is the data set which implies the development
of case classes to be stored and manipulated. Therefore, all major data structures
loaded in this program make use of case classes and data sets.

## Build and run locally

In order to build and run locally the following commands can be used:

```scala
$ sbt compile
$ sbt "run --app-token token_app --data-folder /home/user/kaggle/nyc-parking-violations/data --csv-folder /home/andersonkmi/kaggle/nyc-parking-violations --destination-folder /home/user/temp"
```

The **token_app** refers to a application token that must be obtained from the NYC open data web site. The reason for that
is due to the use of an API for retrieving data directly from them related to violation codes. See the DOF Parking
Violation codes reference below.

The **--data-folder** option indicates the source folder of the CSV files containing the violations. 

The **--csv-folder** option indicates the folder for plate type and state information CSV files.

The **--destination-folder** option indicates where the data transformation results will be written to. If not
informed, the default destination is the project root folder.

## Changelog

All modifications developed for this project are listed on [CHANGELOG.md](CHANGELOG.md)

## References

- [Kaggle NYC parking violations issued](https://www.kaggle.com/new-york-city/ny-parking-violations-issued)
- [Parking Violations Issued - Fiscal Year 2019](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2019/pvqr-7yc4)
- [DOF Parking Violation Codes](https://data.cityofnewyork.us/Transportation/DOF-Parking-Violation-Codes/ncbg-6agr)
