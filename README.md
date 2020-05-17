# Data Engineering Capstone Project

This project contains the code files for the implementation of the Capstone Project for my Udacity Data Engineering Nanodegree.

## Motivation

In this project I wanted to create a database that could be used for analysing historical residential housing transactions in the UK.

## Questions we want to be able to answer

The answers to these questions will be shown at the end of this README file. I wanted to be able to answer questions such as:

1. How have the house prices the UK changed since 1995? Which regions show the greatest growth?
2. What relationship is there between the quality of an area and the house prices?
3. Which months, weeks and days of the week are the best for house sales in terms of volume and prices paid?

## The data model

The entity relationship diagram for this model is shown below.

![ERD](erd.png)

There is a great article on the Thoughspot website detailing [common data modelling traps](https://www.thoughtspot.com/fact-and-dimension/schemas-scale-how-avoid-common-data-modeling-traps). It helped greatly with recognising what issues could arise in designing this schema.

## Data sources

### Houses Price Paid

A `.csv` file of all property sales in the UK in the years 1995-2017 from [HM Land Registry](https://data.gov.uk/dataset/314f77b3-e702-4545-8bcb-9ef8262ea0fd/archived-price-paid-information-residential-property-1995-to-2012). The column descriptions for this data file were also given by this link.

| Column | Description |
|--------|-------------|
|    Transaction unique identifier    |     A reference number which is generated automatically recording each published sale. The number is unique and will change each time a sale is recorded.        |
| Price | Sale price stated on the transfer deed |
| Date of Transfer | Date when the sale was completed, as stated on the transfer deed. |
| Postcode | This is the postcode used at the time of the original transaction. Note that postcodes can be reallocated and these changes are not reflected in the Price Paid Dataset.|
|Property Type| D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other|
|Old/New|Indicates the age of the property and applies to all price paid transactions, residential and non-residential. Y = new build, N = establised residential building|
|Duration|Relates to the tenure: F = Freehold, L= Leasehold etc.|
|PAON|Primary Addressable Object Name. Typically the house number or name.|
|SAON|Secondary Addressable Object Name. Where a property has been divided into separate units (for example, flats), the PAON (above) will identify the building and a SAON will be specified that identifies the separate unit/flat.|
|Street||
|Locality||
|Town/City||
|District||
|County||
|PPD Category Type|Indicates the type of Price Paid transaction. **A** = Standard Price Paid entry, includes single residential property sold for value. **B** = Additional Price Paid entry including transfers under a power of sale/repossessions, buy-to-lets (where they can be identified by a Mortgage) and transfers to non-private individuals.|
|Record Status|Indicates additions, changes and deletions to the records.|

### Postcodes

A `.csv` file for supplementary data on all of the UK postcodes as of Feb 2020 from the [Office of National Statistics](https://geoportal.statistics.gov.uk/datasets/national-statistics-postcode-lookup-february-2020).

Gathering the data had some challenges in its own right. The easiest chunk of data to collect was the single file of all the house sales from 1995-2017. I wanted to then supplement this data with more information on the postcodes of these houses that were sold. For example, the longitude and the latitude of the postcode which would allow a plot of all the housesales on the UK map to be done. This data also came with a host of added benefits such as information on how the areas are classified.

## The data pipeline

I decided to use an Amazon Redshift cluster. The data are downloaded from the mentioned sources and then uploaded to an S3 bucket. The two sets of data are then loaded into two staging tables on an Amazon Redshift cluster. SQL code transformations are executed on the data which is then loaded into final tables as detailed in the schema.

The code is contained across three files:
- `sql_queries.py`: this is the file that contains all of the SQL queries for creating and dropping the tables as well as copying and inserting the data into these tables.
- `create_tables.py`: this file contains the code that creates the tables in the Amazon Refshift cluster.
- `etl.py`: this file contains the code for copying the data from S3 into the staging tables and inserting data from these tables into the final tables.


## How this project would change with extra requirements

1. What about data 100x bigger?

In this case I would likely transfer across to PySpark for processing. At the minute some data cleaning is done within a Python script using pandas dataframes due to their ease and familiarity. However, as the data grows, making use of a dsitributed cluster would be sensible as the local memory would rapidly run out.

2. The data populates a dashboard and must be updated on a daily basis by 7am every day

I would use the Airflow tool to handle this scheduling requirments. A DAG (Directed Acyclic Graph) for the ETL pipeline would be created and set on a daily schedule to run at midnight of each day. To account for the scenario where the pipeline would fail, I would set the DAG to send an email notification after the DAG was to fail for the third time.

3. Database needs to be accessed by 100+ people

I have already implemented the database on an Amazon Redshift cluster using Infrastructure as Code ideas. Accesses for the 100+ data consumers would be granted. A benefit of using AWS here is the ease with which the accesses can be managed for the cluster.

## Updating the data

In an ideal world, we would have real-time data for every component. Getting every house sale as soon as the contract was signed on the dotted line would be great, however there are some more realistic expectations we can put on here.

- **House Sales** - monthly release, thus update this data monthly from the [HM Land Registry](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#current-month-march-2020-data).
- **Postcodes** - the Office of National Statistics realeases a new postcode lookup file every three months [here](https://geoportal.statistics.gov.uk/datasets/4f71f3e9806d4ff895996f832eb7aacf). Update according to this timeline.



## Licenses

Office for National Statistics licensed under the Open Government Licence v.3.0

Contains HM Land Registry data © Crown copyright and database right 2020. This data is licensed under the Open Government Licence v3.0
