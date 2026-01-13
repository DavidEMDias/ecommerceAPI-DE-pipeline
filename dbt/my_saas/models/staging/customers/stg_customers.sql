with source as (
        select *
        from {{ source('raw_saas', 'customers') }} source_table 
)

select
  customer_id,
  company_name,
  country,
  industry,
  company_size,
  signup_date::timestamp as signup_date,
  updated_at::timestamp as updated_at,
  is_churned::boolean as is_churned,
  date_trunc('month', signup_date)::date as signup_month
 from source
 where is_deleted = 'false'