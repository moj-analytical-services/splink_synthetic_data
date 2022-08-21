def parse_point_to_lat_lng(
    pipeline, colname_to_replace, output_df_name, input_table_name="df"
):

    intermediate_df_name_1 = f"space_delimited_coordinates_{colname_to_replace}"

    sql = f"""
    select
        * exclude ({colname_to_replace}),
        list_transform({colname_to_replace}, x ->
            replace(
                replace(x, 'Point(', ''),
                ')', ''))
            as {colname_to_replace}
    from {input_table_name}
    """
    pipeline.enqueue_sql(sql, intermediate_df_name_1)

    intermediate_df_name_2 = f"space_delimited_coordinates_2_{colname_to_replace}"
    sql = f"""
    select
        * exclude ({colname_to_replace}),
        list_transform({colname_to_replace}, x -> str_split(x, ' '))
            as {colname_to_replace}
    from {intermediate_df_name_1}
    """
    pipeline.enqueue_sql(sql, intermediate_df_name_2)

    sql = f"""
    select
        * exclude ({colname_to_replace}),
        list_transform({colname_to_replace}, x -> struct_pack(lat :=x[2], lng:=x[1]))
            as {colname_to_replace}
    from {intermediate_df_name_2}
    """

    pipeline.enqueue_sql(sql, output_df_name)
    return pipeline
