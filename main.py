from etl import alt_source_stage, stage_to_dp

# ETL for the connection to the audit logs
alt_source_stage.run_etl_general()

# ETL for the various dataproducts
# stage_to_dp.run_etl_aggregate()
