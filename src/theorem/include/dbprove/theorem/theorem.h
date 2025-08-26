#pragma once
#include <dbprove/generator/generator_state.h>
#include <dbprove/sql/sql.h>
#include <dbprove/sql/explain/plan.h>
#include <vector>
#include <string>
#include <memory>
#include <functional>
#include <magic_enum/magic_enum.hpp>

namespace dbprove::theorem
{
    class Proof;
    class RunCtx;
    class Theorem;
    class Query;
    class Data;
    class DataExplain;
    class DataQuery;
    using TheoremFunction = std::function<void(Proof& state)>;

    /**
    * The type of Theorem
     */
    enum class Type
    {
        CLI = 1,
        WLM = 2,
        PLAN = 3,
        EE = 4,
        SE = 5,
        UNKNOWN = 0
    };

    inline std::string_view to_string(const Type type) { return magic_enum::enum_name(type); }

    /**
     * Convert from a name to the enum
     * @param type_name Name to convert
     * @return Enum matching the name
     */
    Type typeEnum(const std::string& type_name);

    /**
     * All types by name
     * @return set of all types
     */
    std::set<std::string_view> allTypeNames();

    /**
     * Any datapoint that supports a Proof
     */
    class Data
    {
    public:
        virtual ~Data() = default;

        enum class Type
        {
            EXPLAIN,
            QUERY
        };

        const Type type;

        explicit Data(const Type type)
            : type(type)
        {
        }

        virtual void render(Proof& out)
        {
        }
    };

    /**
     * Explain plans and all the analysis that goes with it
     */
    class DataExplain final : public Data
    {
    public:
        explicit DataExplain(std::unique_ptr<sql::explain::Plan> plan)
            : Data(Type::EXPLAIN)
            , plan(std::move(plan))
        {
        }

        std::unique_ptr<sql::explain::Plan> plan;

        void render(Proof& proof) override;
    };


    class DataQuery final : public Data
    {
    public:
        explicit DataQuery(Query& query)
            : Data(Type::QUERY)
            , query(query)
        {
        }

        Query& query;
        void render(Proof& proof) override;
    };

    enum class Unit
    {
        Rows,
        COUNT,
        Magnitude,
        Plan
    };

    constexpr inline std::string_view to_string(const Unit unit) { return magic_enum::enum_name(unit); }

    /**
     * A Proof is the holder of all data that is the result of proving a theorem
     */
    class Proof
    {
    public:
        Proof(const Theorem& theorem, RunCtx& parent)
            : theorem(theorem)
            , state(parent)
        {
        }

        ~Proof();
        const Theorem& theorem;
        std::vector<std::unique_ptr<Data>> data;
        [[nodiscard]] sql::ConnectionFactory& factory() const;
        Proof& ensure(const std::string& table);
        /**
         * Make sure the schema exists
         * @param schema To create if not there
         * @return
         */
        Proof& ensureSchema(const std::string& schema);
        void render();
        [[nodiscard]] std::ostream& console() const;
        /**
         * Write data into the csv stream for later merging and processing
         * @param key Unique name (in the context of the  theorem) of the measurement
         * @param value The measurement. Can be of any type, but must cast to string
         * @param unit Unit of measurement
         */
        void writeCsv(const std::string& key, std::string value, Unit unit) const;
        [[nodiscard]] std::ostream& csv() const;

    private:
        RunCtx& state;
    };


    class Theorem
    {
    public:
        Theorem(const Type type,
                std::string theorem,
                std::string description,
                const TheoremFunction& func
        )
            : type(type)
            , name(std::move(theorem))
            , description(std::move(description))
            , func(func)
        {
        }

        Type type;
        std::string name;
        std::string description;
        TheoremFunction func;
    };


    /**
     * RunContext must be passed into the Theorem prove function to provide the environment that is used for proving
     * It is also where the proofs are kept for returning to caller
     */
    class RunCtx
    {
        class CsvWriter;
        std::unique_ptr<CsvWriter> writer;

    public:
        const sql::Engine& engine;
        const sql::Credential& credentials;
        generator::GeneratorState& generator;
        sql::ConnectionFactory factory{engine, credentials};
        std::ostream& console;
        std::ostream& csv;
        std::vector<std::unique_ptr<Proof>> proofs;
        void writeCsv(const std::vector<std::string_view>& values) const;
        RunCtx(
            const sql::Engine& engine,
            const sql::Credential& credentials,
            generator::GeneratorState& generator,
            std::ostream& console,
            std::ostream& csv
        );

        ~RunCtx();
    };


    /**
     * Call this before using the library
     */
    void init();
    /**
     * Parse a list of theorems and turn them into properly typed theorems
     * @param theorems List of strings to parse
     * @return The Theorems to run based on the input
     */
    std::vector<const Theorem*> parse(const std::vector<std::string>& theorems);


    /**
     * Run all theorems provided.
     * @param theorems Theorems per parse call
     * @param input_state Caller supplied state with all the info needed to run
     */
    void prove(const std::vector<const Theorem*>& theorems, RunCtx& input_state);
}
