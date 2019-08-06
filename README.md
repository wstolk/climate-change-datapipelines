# climate-change-datapipelines
Apache Airflow data pipelines to collect and process data about climate change.

## Goal of the project
This project has been initiated to finalise the Udacity Nanodegree of Data Engineering.
Goal of the project is to build data pipelines for retrieving and processing 
data about climate change.

## Structure
The structure of this project can be divided in two parts: 
1. Development environment
2. Data pipelines

### Development environment
The development environment is build on top of Docker for easier deployment. 
It uses a stripped down version of the 
[Puckel Airflow docker image](https://github.com/puckel/docker-airflow) 
as a starting point. This includes the following containers:
* Postgres database
* Redis as a message broker
* Airflow scheduler
* Airflow webserver
* Airflow worker

### Data pipelines
The data pipelines have a number of goals:
* Retrieving data out of sources:
    * API's
    * [Datahub.io](https://datahub.io)
* Storing the data to a data lake (AWS S3)
* Copy data from the data lake to staging tables in the data warehouse (AWS Redshift)
* Insert data from the staging tables to fact and dimension tables
* Validate data quality

## Data sources
Data packages are retrieved from [Datahub.io](https://datahub.io) 
using the Python package [datapackage](https://github.com/frictionlessdata/datapackage-py).

The following data packages have been used for this project:
* [Global Temperature Time Series](https://datahub.io/core/global-temp)
* [Glacier Mass Balance](https://datahub.io/core/glacier-mass-balance)
* [Sea Level Rise](https://datahub.io/core/sea-level-rise)
* [CO2 PPM - Trends in Atmospheric Carbon Dioxide](https://datahub.io/core/co2-ppm)
* [World Population Growth](https://datahub.io/core/population) 