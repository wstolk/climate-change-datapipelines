# climate-change-datapipelines
Apache Airflow data pipelines to collect and process data about climate change.

## Goal of the project
This project has been initiated to finalise the Udacity Nanodegree of Data Engineering.
Goal of the project is to build data pipelines for retrieving and processing 
data about climate change.

## Planning
1. Gather data sources
2. Setup development environment
3. Analyse data sources using Jupyter Notebooks
4. Define data model
5. Build data pipelines
6. Validate results

### 1. Data sources
Data packages are retrieved from [Datahub.io](https://datahub.io) 
using the Python package [datapackage](https://github.com/frictionlessdata/datapackage-py).

The following data packages have been used for this project:
* [Global Temperature Time Series](https://datahub.io/core/global-temp)
* [Glacier Mass Balance](https://datahub.io/core/glacier-mass-balance)
* [Sea Level Rise](https://datahub.io/core/sea-level-rise)
* [CO2 PPM - Trends in Atmospheric Carbon Dioxide](https://datahub.io/core/co2-ppm)
* [World Population Growth](https://datahub.io/core/population) 

### 2. Development environment
The development environment is build on top of Docker for easier deployment. 
It uses a stripped down version of the 
[Puckel Airflow docker image](https://github.com/puckel/docker-airflow) 
as a starting point. This includes the following containers:
* Postgres database
* Redis as a message broker
* Airflow scheduler
* Airflow webserver
* Airflow worker

### 3. Dataset analysis
Each data set will be analysed individually using Jupyter Notebooks. 
All notebooks can be found in the `/notebooks` folder.

#### 3.1 Global temperature time series
The data package contains datasets for the temperature deviations between 1882 and 2016.
There are two datasets available: the yearly temperature deviation and the 
monthly temperature deviation. 

After analysis, we see that the global temperature has roughly increased with one degree Celsius since the global mean of 1951 to 1980:
![Global temperature](./notebooks/gcag_gistemp_analysis.png)

### Data pipelines
The data pipelines have a number of goals:
* Retrieving data out of sources:
    * API's
    * [Datahub.io](https://datahub.io)
* Storing the data to a data lake (AWS S3)
* Copy data from the data lake to staging tables in the data warehouse (AWS Redshift)
* Insert data from the staging tables to fact and dimension tables
* Validate data quality