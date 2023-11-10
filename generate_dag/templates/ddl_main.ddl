CREATE TABLE <dag_name> (
    ################################################################################
    # Add columns here and COMMENT 'UNIQUE KEY' for the primary keys for the table #
    #################################### EXAMPLE ###################################
    customer_id STRING COMMENT 'UNIQUE KEY',
    searched_domain_name STRING COMMENT 'UNIQUE KEY',
    shopper_id STRING,
    searched_ts TIMESTAMP,
    #################### Remove commented section before push! #####################
    ################################################################################
    etl_delete_flag BOOLEAN,
    etl_first_load_utc_ts TIMESTAMP,
    etl_last_refresh_utc_ts TIMESTAMP,
    etl_last_refresh_id BOOLEAN,
    etl_build_utc_ts TIMESTAMP
)
PARTITIONED BY (partition_utc_date DATE)
STORED AS PARQUET
LOCATION '{location}'
TBLPROPERTIES
    (
        'processing_dag_name'='<dag_name>'
    )
