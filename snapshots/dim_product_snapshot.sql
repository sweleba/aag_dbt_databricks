{% snapshot dim_product_snapshot %}

{{
  config(
    unique_key='product_key',
    strategy='timestamp',
    updated_at='last_updated_at',
  )
}}

select * from {{ ref('dim_product') }}

{% endsnapshot %}
