class SQLTask:
    def __init__(self, sql, output_table_name):
        self.sql = sql
        self.output_table_name = output_table_name


class SQLPipeline:
    def __init__(self, con):
        self.con = con
        self.queue = []

    def enqueue_sql(self, sql, output_table_name):
        sql_task = SQLTask(sql, output_table_name)
        self.queue.append(sql_task)

    def generate_pipelined_sql(self):

        with_parts = self.queue[:-1]
        last_part = self.queue[-1]

        with_parts = [f"{p.output_table_name} as ({p.sql})" for p in with_parts]
        with_parts = ", \n".join(with_parts)
        if with_parts:
            with_parts = f"WITH {with_parts} "

        final_sql = with_parts + last_part.sql

        return final_sql

    def execute_pipeline_in_parts(self):
        for sql_task in self.queue:

            print("---")
            print(sql_task.output_table_name)
            print(sql_task.sql)

            df_arrow = self.con.execute(sql_task.sql).fetch_arrow_table()
            self.con.register(sql_task.output_table_name, df_arrow)

            df_to_show = self.con.execute(
                f"select * from {sql_task.output_table_name} limit 5"
            ).fetch_df()
            display(df_to_show)
        return df_arrow.to_pandas()

    def execute_pipeline(self):
        sql = self.generate_pipelined_sql()
        return self.con.execute(sql)

    def reset(self):
        self.queue = []
