-- select * from walmart.bronze.customers;

select * from {{ source('walmart_databricks', 'customers') }};