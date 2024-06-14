_**Azure-DE-tokyoOlympic_Project**_

**Project Description**:- Performed basic data engineering techniques using Azure services and Kaggle dataset.

**Source**  - Kaggle Dataset 
**Sink** - Azure datalake gen2.
**Tech Stack** - python, pyspark, Azure Data factory , Azure Synapse Analytics, Azure Databricks.

**Data ingestion** -> Created a ADF pipeline which will pickup data from the Source files and dump them in datalake as raw data.
**Data Transformation** -> Performed Data transformation - ( filtering , sorting, aggregation) using Azure Databricks. Further written the transformed dataframe into Datalake uder transformedData directory.
**SQL scripts** -> Used Azure Synapse Analytics dedicated SQL pool for creating dtabase and tables to derive insights from the transformed Data.



