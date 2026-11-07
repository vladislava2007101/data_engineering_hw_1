with recursive tree as (

    select
        category_id,
        category_name,
        parent_category_id,
        null::text          as parent_category_name,
        category_name       as root_category_name,
        1                   as category_level
    from {{ ref('stg_shop__product_categories') }}
    where parent_category_id is null
    union all

    select
        child.category_id,
        child.category_name,
        child.parent_category_id,
        parent.category_name        as parent_category_name,
        parent.root_category_name        as root_category_name,
        parent.category_level + 1   as category_level
    from {{ ref('stg_shop__product_categories') }} as child
    join tree as parent
        on child.parent_category_id = parent.category_id
)

select * from tree
