### Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The main goal of this project is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Sparkify also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Structure
The project template package contains three major components for the project:

1. The dag template has all the imports and task templates in place: file `data_pipeline_dag.py`
2. The plugins/operators folder with operator templates files:
- `data_quality.py`: Defines DataQualityOperator to run data quality checks on all tables passed as parameter.
- `load_dimension.py`: Defines LoadDimensionOperator to load a dimension table from staging tables.
- `load_fact.py`: Defines LoadFactOperator to load fact table from staging tables.
- `stage_redshift.py`: Defines StageToRedshiftOperator to copy JSON data from S3 to staging tables in the Redshift via copy command.
3. A helper class for the SQL transformations
- `sql_queries.py`: Contains SQL queries for the ETL pipeline.
