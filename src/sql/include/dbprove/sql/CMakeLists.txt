target_sources(${_targetName}
        PUBLIC FILE_SET installed FILES
        column_base.h
        connection_base.h
        connection_factory.h
        credential.h
        engine.h
        integer_type_def.h
        query.h
        result_base.h
        row_base.h
        row_iterator.h
        sql_exceptions.h
        sql_type.h
        sql.h
        explain/node.h
        explain/node_type.h
)

