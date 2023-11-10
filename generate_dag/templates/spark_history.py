import argparse

from sparkutils import create_spark_session, create_table_path, transform_stg


def main(database_name, table_name, env):
    table_path = create_table_path(env, database_name, table_name)
    spark, logger = create_spark_session(table_name)

    # ####### Put all the ETL code here #########
    # ## The last DF must be called source_df ###
    # ### Remove commented code before push! ####
    source_df = spark.sql('''
        SELECT
            ...,
            CURRENT_TIMESTAMP() AS etl_build_utc_ts,
            CURRENT_DATE() AS partition_utc_date
        FROM ...
    ''')

    transform_stg(spark, logger, database_name, table_name, table_path, source_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='List the pyspark parameters')
    parser.add_argument('-d', '--database-name', required=True, help='Name of Database')
    parser.add_argument('-t', '--table-name', required=True, help='Name of Table')
    parser.add_argument('-e', '--env', choices=['dev-private', 'stage', 'prod'], help='AWS environment',
                        default='dev-private')
    args = parser.parse_args()
    main(args.database_name, args.table_name, args.env)
