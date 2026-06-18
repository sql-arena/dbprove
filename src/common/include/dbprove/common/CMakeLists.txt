target_sources(${_targetName}
        PUBLIC FILE_SET installed FILES
        config.h
        storage_variant.h
        string.h
        tree_node.h
        null_stream.h
        log_formatter.h
        aws_bucket.h
        docker.h
        file_utility.h
        json_utility.h
        table_data_conventions.h
)
