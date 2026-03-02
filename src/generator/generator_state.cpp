#include "generator_state.h"

#include <fstream>

#include "generated_table.h"
#include "dbprove/sql/connection_factory.h"
#include <dbprove/sql/sql.h>
#include <plog/Log.h>
#include <google/cloud/storage/client.h>
#include <zip.h>

#include "dbprove/common/file_utility.h"
#include "dbprove/generator/test.h"

namespace generator
{
    std::map<std::string_view, GeneratedTable*>&
    available_tables()
    {
        static std::map<std::string_view, GeneratedTable*> registry;
        return registry;
    }

    auto& pk_to_fk()
    {
        static std::map<std::string_view, std::vector<sql::ForeignKey>> map;
        return map;
    }

    auto& fk_to_pk()
    {
        static std::map<std::string_view, std::vector<sql::ForeignKey>> map;
        return map;
    }


    void GeneratorState::downloadFromCloud(const std::string_view schemaName,
        const std::string_view tableName,
        const std::string_view relativePath)
    {
        if (cloudProvider() == CloudProvider::AWS) {
            PLOGI << "Cloud provider is AWS (S3), skipping local download as bulk load can use S3 directly.";
            const auto fullTablename = std::format("{}.{}", schemaName, tableName);
            // We still need to register it as "generated" so the load() logic knows it's ready to be loaded via bulkLoad
            // But we don't have a local path. Connection::bulkLoad for Databricks currently ignores the source_paths 
            // and uses a hardcoded S3 path.
            table(tableName).is_generated = true;
            return;
        }

        if (cloudProvider() != CloudProvider::GCS) {
            throw std::runtime_error("downloadFromCloud currently supports only Google Cloud Storage (GCS) for local downloads");
        }

        dbprove::common::make_directory(basePath_);

        const std::string bucket = "sql-arena-data";
        std::string object = std::format("{}/{}{}.csv.zip",
             schemaName, relativePath, tableName);;
        const auto fullTablename = std::format("{}.{}", schemaName, tableName);
        const auto final_csv_path = csvPath(tableName);
        const auto base_dir = basePath();
        const auto downloadPath = base_dir / std::format("{}.csv.zip", tableName);
        const auto zipCSVFileName = std::format("{}.csv", tableName);

        if (!std::filesystem::exists(downloadPath)) {
            PLOGI << "Downloading GCS object " << object << " to " << downloadPath.string();
            namespace gcs = ::google::cloud::storage;
            auto client = gcs::Client::CreateDefaultClient().value();
            std::ofstream ofs(downloadPath, std::ios::binary);
            if (!ofs.is_open()) {
                throw std::runtime_error("Failed to open local zip file for writing: " + downloadPath.string());
            }
            auto reader = client.ReadObject(bucket, object);
            if (!reader.status().ok()) {
                throw std::runtime_error("GCS ReadObject failed: " + reader.status().message());
            }
            std::vector<char> buffer(1 << 16);
            while (!reader.eof()) {
                reader.read(buffer.data(), buffer.size());
                auto gcount = reader.gcount();
                if (gcount > 0)
                    ofs.write(buffer.data(), gcount);
            }
            ofs.close();
        }

        PLOGI << "Download complete. Unzipping...";
        int err = 0;
        zip_t* za = zip_open(downloadPath.string().c_str(), ZIP_RDONLY, &err);
        if (!za) {
            throw std::runtime_error("Failed to open zip archive: " + downloadPath.string());
        }

        zip_stat_t st{};
        if (zip_stat(za, zipCSVFileName.c_str(), 0, &st) != 0) {
            zip_close(za);
            const auto msg = std::format("File '{}' not found inside zip: {}", zipCSVFileName, downloadPath.string());
            throw std::runtime_error(msg);
        }

        zip_file_t* zf = zip_fopen(za, zipCSVFileName.c_str(), 0);
        if (!zf) {
            zip_close(za);
            const auto msg = std::format("Failed to open '{}' from zip: {}", zipCSVFileName, downloadPath.string());
            throw std::runtime_error(msg);
        }

        std::ofstream out_csv(final_csv_path, std::ios::binary);
        if (!out_csv.is_open()) {
            zip_fclose(zf);
            zip_close(za);
            throw std::runtime_error("Failed to create extracted file: " + final_csv_path.string());
        }

        std::vector<char> zbuf(1 << 16);
        zip_int64_t n;
        while ((n = zip_fread(zf, zbuf.data(), static_cast<zip_uint64_t>(zbuf.size()))) > 0) {
            out_csv.write(zbuf.data(), n);
        }
        out_csv.close();
        zip_fclose(zf);
        zip_close(za);

        PLOGI << "Unzip complete. CSV available at: " << final_csv_path.string();
        registerGeneration(fullTablename, final_csv_path);
    }

    GeneratorState::GeneratorState(const std::filesystem::path& basePath, const CloudProvider dataProvider,
                                   std::string dataPath)
        : basePath_(basePath)
        , dataProvider_(dataProvider)
        , dataPath_(std::move(dataPath))
    {
    }

    GeneratorState::~GeneratorState()
    {
    }

    void GeneratorState::ensure(const std::string_view table_name, sql::ConnectionFactory& conn)
    {
        std::vector table_names{table_name};
        ensure(std::span(table_names), conn);
    }

    void GeneratorState::ensure(const std::span<std::string_view>& table_names, sql::ConnectionFactory& conn)
    {
        for (auto table_name : table_names) {
            sql::checkTableName(table_name);
            if (ready_tables_.contains(table_name)) {
                continue;
            }
            std::unique_ptr<sql::ConnectionBase> cn = conn.create();
            load(table_name, *cn);
            
            // If load() marked the table as ready, we don't need to generate it
            if (!ready_tables_.contains(table_name)) {
                generate(table_name, cn.get());
                // We mark it ready after generation
                ready_tables_.insert(std::string(table_name));
            }

            declareKeys(table_name, *cn);
        }
    }

    bool GeneratorState::contains(const std::string_view table_name)
    {
        sql::checkTableName(table_name);
        return available_tables().contains(table_name);
    }


    sql::RowCount generator::GeneratorState::generate(const std::string_view table_name, sql::ConnectionBase* conn)
    {
        sql::checkTableName(table_name);
        if (!contains(table_name)) {
            throw std::runtime_error("Generator not found for table: " + std::string(table_name));
        }

        const auto target_row_count = table(table_name).row_count;
        const auto file_name = csvPath(table_name);
        if (exists(file_name) && file_size(file_name) > 0) {
            // NOTE: even a zero row file will still have a header.
            PLOGI << "Table: " << table_name << " input already exists.";
            table(table_name).path = file_name;
            return target_row_count;
        }
        PLOGI << "Table: " << table_name << " input being generated...";
        table(table_name).generator(*this, conn);

        return target_row_count;
    }

    sql::RowCount GeneratorState::load(const std::string_view table_name, sql::ConnectionBase& conn)
    {
        sql::checkTableName(table_name);
        PLOGD << "Ensuring table: " << table_name;
        auto existing_rows = conn.tableRowCount(table_name);
        if (!existing_rows) {
            PLOGI << "Table: " << table_name << " does not exist. Constructing it from DDL";
            const auto table_ddl = table(table_name).ddl;
            PLOGD << "Executing DDL: " << table_ddl;
            conn.executeDdl(table_ddl);
            existing_rows = conn.tableRowCount(table_name);
            if (!existing_rows) {
                PLOGE << "Table: " << table_name << " still does not exist after DDL!";
            }
        } else {
            PLOGD << "Table: " << table_name << " already exists with " << *existing_rows << " rows";
        }
        const auto expected_rows = table(table_name).row_count;

        // If it's a generated table, it must have EXACTLY the right amount of rows (roughly)
        if (table(table_name).is_generated) {
            if (existing_rows.value_or(0) == 0 || existing_rows.value() < 0.9 * expected_rows || existing_rows.value() > 1.1 * expected_rows) {
                if (exists(table(table_name).path)) {
                    PLOGI << "Table: " << table_name << " exists but has incorrect row count (" << existing_rows.value_or(0)
                          << "). Loading it from file: " << table(table_name).path.string() << "...";
                    conn.bulkLoad(table_name, {table(table_name).path});
                } else {
                    PLOGI << "Table: " << table_name << " needs re-generation (count: " << existing_rows.value_or(0) << ")";
                    return 0; // Trigger generate() in ensure()
                }
            }
        } else {
            // If it's NOT a generated table (it uses a generator function), we are more lenient if it already has rows.
            // But if it has 0 rows, we definitely need to run the generator.
            if (existing_rows.value_or(0) == 0) {
                PLOGI << "Table: " << table_name << " has no rows and no generator file. Triggering generator function...";
                return 0; // Trigger generate() in ensure()
            }
            
            // If it has rows, but not exactly what we expected, we still accept it as "ready" for now,
            // because running the generator function might be expensive or append data if not idempotent.
            // The user can always manually TRUNCATE if they want a fresh start.
            if (existing_rows.value() != expected_rows) {
                PLOGW << "Table: " << table_name << " has " << existing_rows.value() << " rows, but we expected " << expected_rows << ". Continuing anyway.";
            }
        }

        PLOGI << "Table: " << table_name << " is already in the database with " + std::to_string(
            existing_rows.value()) + " rows";

        ready_tables_.insert(std::string(table_name));
        return existing_rows.value();
    }

    void GeneratorState::registerGeneration(const std::string_view table_name, const std::filesystem::path& path) const
    {
        sql::checkTableName(table_name);
        table(table_name).path = path;
        table(table_name).is_generated = true;
    }

    void GeneratorState::declareKeys(const std::string_view table_name, sql::ConnectionBase& conn) const
    {
        // Key up anything I point at that already exists
        if (fk_to_pk().contains(table_name)) {
            for (auto& fk : fk_to_pk().at(table_name)) {
                if (ready_tables_.contains(fk.pk_table_name)) {
                    conn.declareForeignKey(table_name, fk.fk_columns, fk.pk_table_name, fk.pk_columns);
                }
            }
        }

        // Key up anything existing, which points at me
        if (pk_to_fk().contains(table_name)) {
            for (auto& pk : pk_to_fk().at(table_name)) {
                if (ready_tables_.contains(pk.fk_table_name)) {
                    conn.declareForeignKey(pk.fk_table_name, pk.fk_columns, table_name, pk.pk_columns);
                }
            }
        }
    }

    GeneratedTable& GeneratorState::table(const std::string_view table_name) const
    {
        sql::checkTableName(table_name);
        if (!contains(table_name)) {
            throw std::runtime_error(
                "Table not found: " + std::string(table_name) +
                ". Did you forget to call or include the appropriate REGISTER_GENERATOR");
        }
        return *available_tables().at(table_name);
    }

    Registrar::Registrar(const std::string_view table_name, const std::string_view ddl, const GeneratorFunc& f,
                         const sql::RowCount rows)
    {
        sql::checkTableName(table_name);
        available_tables().emplace(table_name, new GeneratedTable{table_name, ddl, f, rows});
    }


    KeyRegistrar::KeyRegistrar(std::string_view fk_table_name, const std::vector<std::string_view>& fk_column_names,
                               std::string_view pk_table_name, const std::vector<std::string_view>& pk_column_names)
    {
        sql::checkTableName(fk_table_name);
        sql::checkTableName(pk_table_name);
        const sql::ForeignKey fk{fk_table_name, fk_column_names, pk_table_name, pk_column_names};
        fk_to_pk()[fk_table_name].push_back(fk);
        pk_to_fk()[pk_table_name].push_back(fk);
    }
}
