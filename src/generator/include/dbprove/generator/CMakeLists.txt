target_sources(${_targetName}
        PUBLIC FILE_SET installed FILES
        generated_table.h
        generator_state.h
        job.h
        sql_resources.h
        tpch.h
)
