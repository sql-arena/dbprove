target_sources(${_targetName}
        PUBLIC FILE_SET installed FILES
        generated_table.h
        generator_state.h
        job.h
        scale.h
        sql_resources.h
        test.h
        tpch.h
)
