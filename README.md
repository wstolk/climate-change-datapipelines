# World Development Analysis
![Airflow Overview](./resources/airflow_overview.png)

Apache Airflow data pipelines to collect and process data about world development and climate change.

## Goal of the project
This project has been initiated to finalise the Udacity Nanodegree of Data Engineering.
Goal of the project is to build data pipelines for retrieving and processing 
data about world development and it's correlation to climate changes.

## Planning
1. [Gather data sources](#1-data-sources)
2. [Setup development environment](#2-development-environment)
3. [Analyse data sources using Jupyter Notebooks](#3-dataset-analysis)
4. [Define data model](#4-data-model)
5. [Build Airflow operators and plugins](#5-airflow-operators-and-plugins)
6. [Build data pipelines](#6-data-pipelines-dags)
7. [Get it up and running yourself!](#7-get-it-up-and-running-yourself)

### 1. Data sources
Most data packages are retrieved from [Datahub.io](https://datahub.io) 
using the Python package [datapackage](https://github.com/frictionlessdata/datapackage-py).

The world development indicators data package has been retrieved from [Kaggle.com](https://kaggle.com).
This dataset has to be downloaded manually and stored to a root folder within the project called `/data/`.

The following data packages have been used for this project:
* [Global Temperature Time Series](https://datahub.io/core/global-temp)
* [Glacier Mass Balance](https://datahub.io/core/glacier-mass-balance)
* [Sea Level Rise](https://datahub.io/core/sea-level-rise)
* [CO2 PPM - Trends in Atmospheric Carbon Dioxide](https://datahub.io/core/co2-ppm)
* [World Population Growth](https://datahub.io/core/population) 
* [World Development Indicators](https://www.kaggle.com/worldbank/world-development-indicators)

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

Information about dataset:
* 3 columns
* 3288 rows
* Format: CSV

Interesting columns are:
* Date
* GCAG (combined dataset for global temperature anomaly data from GHCN and ICOADS)
* GISTEMP (combined land-surface air and sea-surface water temperature anomalies from NASA's GISS)

After analysis, we see that the global temperature has roughly increased with one degree Celsius since the global mean of 1951 to 1980:

![Global temperature](./notebooks/gcag_gistemp_analysis.png)

#### 3.2 Glacier mass balance time series
The data package contains a dataset with the average cumulative mass balance of Glaciers worldwide from 1945 - 2014.
1945 is the base year for the dataset. Measurements are in meters of water. This represents the global changes
in the average thickness of glaciers.

Information about dataset:
* 3 columns
* 70 rows
* Format: CSV

Interesting columns are:
* Year
* Mean cumulative mass balance

The data clearly shows a massive decrease of roughly 30 meters since 1945 in the average thickness of glaciers worldwide:

![Global decrease glacier thickness](./notebooks/glacier_mean_cumulative_mass_balance.png)

#### 3.3 Sea level rise
The data package contains a dataset with the global mean of the sea level from 1886 to 2015.
The base year of the dataset lies around 1986.

Information about dataset:
* 3 columns
* 1608 rows
* Format: CSV

Interesting columns are:
* Date
* Global mean sea level (millimeters)

The data shows an increase of almost 60 millimeters since the base year of 1985.
When comparing it to the start of measurements (1886), the sea level has increased with more than 210 millimeters.

![Global sea level rise](./notebooks/sea_level_rise.png)

#### 3.4 CO2 PPM 
The data package contains a dataset with the trend of carbon dioxide PPM since 1958.
The carbon dioxide PPM is defined as the number of CO2 molecules divided by all molecules in the air
after water vapor has been removed.
 
Information about dataset:
* 6 columns
* 727 rows
* Format: CSV

Interesting columns are:
* Date
* Interpolated (CO2 PPM)
* Trend

The data shows an increase of almost 100 PPM of CO2 (300 PPM to 400 PPM) in the air since 
the base year of 1959.

![CO2 PPM in air](./notebooks/co2_ppm_rise.png)

#### 3.5 Population growth 
The data package contains a dataset with the population growth per country, per region, per continent and globally.
 
Information about dataset:
* 4 columns
* 14885 rows
* Format: CSV

Interesting columns are:
* Country name
* Country code
* Year
* Population

The data shows an increase of over 4 billion people since the base year of 1960. That is an increase of more than 200%.

![Global population growth](./notebooks/population_growth_globally.png)

#### 3.6 World Development Indicators
The data package contains 6 datasets, available as seperate CSV's or packaged in a SQLite database. 
We will use the CSV's for ease of use. The package consists of the following datasets:

1. Country
2. CountryNotes
3. Footnotes
4. Indicators
5. Series
6. SeriesNotes 

We will focus on three of the six datasets: country, indicators and series.

##### 3.6.1 Country

The country dataset contains 247 rows and 31 columns. The interesting columns are:

* CountryCode
* ShortName
* Alpha2Code
* CurrencyUnit
* Region
* IncomeGroup

##### 3.6.2 Indicators

The indicators dataset contains 6 columns and roughly 5.660.000 rows. The interesting columns are:

* CountryCode
* IndicatorCode
* Year
* Value

##### 3.6.3 Series

The series dataset contains information about the indicators. It consists of 20 columns and 1345 rows. 
Interesting columns are:

* SeriesCode
* Topic
* IndicatorName
* Periodicity
* BasePeriod
* AggregationMethod

The UnitOfMeasure column would have been a good addition to the above mentioned columns, however, 
it rarely has a value and is therefore left out.

### 4. Data Model

The indicator dataset has been chosen as the fact table. The other datasets are used as dimension tables. 
Most relations between the fact and dimension tables are based on date or country code.

![Data Model](./resources/world_development_data_model.png)

### 5. Airflow operators and plugins

When looking at the data pipelines needed, there are three plugins we need to build. 
Each plugin can have multiple operators:
1. Upload datapackage resource to S3
2. Upload local file to S3
3. Internal Redshift operations
    1. Copy from S3 to staging tables
    2. Copy from staging tables to production tables
    3. Validate data quality

### 6. Data pipelines (DAG's)

The data pipelines have a number of goals:
* Retrieving data out of sources:
    * API's
    * [Datahub.io](https://datahub.io)
* Storing the data to a data lake (AWS S3)
* Copy data from the data lake to staging tables in the data warehouse (AWS Redshift)
* Insert data from the staging tables to fact and dimension tables
* Validate data quality

In total, we have built three DAG's:
1. `redshift_create_tables.py` for creating the staging and production tables in Redshift
2. `datahub_datasets.py` for processing all datasets from [Datahub.io](https://datahub.io) to production tables in Redshift
3. `local_datasets.py` for processing all local datasets to production tables in Redshift

#### 6.1 Datahub datasets DAG
![Datahub datasets DAG](./resources/dag_datahub.png)

The Datahub datasets DAG is a generic DAG. This means that one codebase generates mulitple DAG's through a for loop.
All the datasets are listed in the `datasets` variable, each containing nine variables:
1. `id`: ID of the DAG.
2. `url`: URL to the datapackage JSON.
3. `resource`: resource within the datapackage that needs to be processed.
4. `headers`: a list of header names. Make sure this matches the number of headers expected!
5. `s3_filename`: name/path in which to store the resource in S3.
6. `staging_table`: name of the staging table in Redshift.
7. `prod_table`: name of the production table in Redshift.
8. `prod_columns`: tuple of column names from the Redshift table. This has to be a tuple!
9. `insert_query`: string containing a SQL select query to select the data from the staging table that you want to insert into the production table.

#### 6.2 Local datasets DAG
![Local datasets DAG](./resources/dag_localfiles.png)
The local datasets DAG is a generic DAG. This means that one codebase generates mulitple DAG's through a for loop.
All the local datasets are listed in the `datasets` variable, each containing seven variables:
1. `id`: ID of the DAG.
2. `path`: path to the file on your filesystem.
3. `s3_filename`: name/path in which to store the resource in S3.
4. `staging_table`: name of the staging table in Redshift.
5. `prod_table`: name of the production table in Redshift.
6. `prod_columns`: tuple of column names from the Redshift table. This has to be a tuple!
7. `insert_query`: string containing a SQL select query to select the data from the staging table that you want to insert into the production table.

### 7. Get it up and running yourself!
Prerequisites:
* Computer with at least 8GB of RAM and 5GB of free storage available
* [Docker](https://www.docker.com/) with [docker-compose](https://docs.docker.com/compose/) (docker-compose should be included out of the box)

Steps:
1. Clone this repository
2. Download the [Kaggle dataset](https://www.kaggle.com/worldbank/world-development-indicators) and store it to a folder called "data" in the root of this project
3. Within your terminal, execute the following commands:
    1. `cd ./path/to/the/repository`
    2. `docker-compose up` (this might take a minute or two to finish booting the containers)
4. Navigate to [localhost:8080](http://localhost:8080), you should be presented with the main Airflow overview
5. Add the following variables under Admin/Variables:
    1. `redshift_arn`: containing the ARN of your Redshift role
    2. `redshift_schema`: name of your Redshift schema, usually the value is "public"
    3. `s3_bucket`: name of your S3 bucket where the files will be stored
6. Add the following connections under Admin/Connections:
    1. `redshift_conn`, containing the connection details of your Redshift instance
    2. `s3_conn`, containing the connection details to your S3 bucket