CREATE TABLE <dag_name>_history (
    ################################################################################
    # Add columns here and COMMENT 'UNIQUE KEY' for the primary keys for the table #
    #################################### EXAMPLE ###################################
    customer_id STRING COMMENT 'UNIQUE KEY',
    searched_domain_name STRING COMMENT 'UNIQUE KEY',
    shopper_id STRING,
    searched_ts TIMESTAMP,
    #################### Remove commented section before push! #####################
    ################################################################################
    etl_build_utc_ts TIMESTAMP
)
PARTITIONED BY (partition_utc_date DATE)
STORED AS PARQUET
LOCATION '{location}'
TBLPROPERTIES
    (
        'processing_dag_name'='<dag_name>'
    )
