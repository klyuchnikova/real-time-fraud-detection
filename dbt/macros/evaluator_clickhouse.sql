{% macro clickhouse__recursive_dag() %}
{# Keep traversal identifiers distinct from final output aliases for ClickHouse CTE resolution. #}
{% set depth = var('max_depth_dag') | int %}
{% if depth < 2 or depth < var('chained_views_threshold') | int %}
    {{ exceptions.raise_compiler_error('max_depth_dag must cover chained_views_threshold') }}
{% endif %}
with recursive traversal as (
    select distinct
        resource_id as ancestor_key,
        resource_id as descendant_key,
        materialized as descendant_materialization,
        toUInt32(0) as hop_count,
        [resource_name] as route,
        cast(null as Nullable(Bool)) as view_chain
    from {{ ref('int_direct_relationships') }}
    where resource_type != 'test'

    union all

    select
        previous.ancestor_key as ancestor_key,
        edge.resource_id as descendant_key,
        edge.materialized as descendant_materialization,
        toUInt32(previous.hop_count + 1) as hop_count,
        arrayPushBack(previous.route, edge.resource_name) as route,
        toNullable(
            if(previous.descendant_materialization in ('view', 'ephemeral'),
               coalesce(previous.view_chain, true), false)
        ) as view_chain
    from traversal as previous
    inner join {{ ref('int_direct_relationships') }} as edge
        on previous.descendant_key = edge.direct_parent_id
    where edge.resource_type != 'test' and previous.hop_count < {{ depth - 1 }}
),
all_relationships as (
    select
        p.resource_id as parent_id,
        p.resource_name as parent,
        p.resource_type as parent_resource_type,
        p.model_type as parent_model_type,
        p.materialized as parent_materialized,
        p.is_public as parent_is_public,
        p.access as parent_access,
        p.source_name as parent_source_name,
        p.file_path as parent_file_path,
        p.directory_path as parent_directory_path,
        p.file_name as parent_file_name,
        p.is_excluded as parent_is_excluded,
        c.resource_id as child_id,
        c.resource_name as child,
        c.resource_type as child_resource_type,
        c.model_type as child_model_type,
        c.materialized as child_materialized,
        c.is_public as child_is_public,
        c.access as child_access,
        c.source_name as child_source_name,
        c.file_path as child_file_path,
        c.directory_path as child_directory_path,
        c.file_name as child_file_name,
        c.is_excluded as child_is_excluded,
        toInt32(walk.hop_count) as distance,
        walk.route as path,
        walk.view_chain as is_dependent_on_chain_of_views
    from traversal as walk
    inner join {{ ref('int_all_graph_resources') }} as p on walk.ancestor_key = p.resource_id
    inner join {{ ref('int_all_graph_resources') }} as c on walk.descendant_key = c.resource_id
)
{% endmacro %}
