#pragma once
#include <dbprove/generator/generator_state.h>
#include <dbprove/sql/sql.h>
#include <dbprove/sql/explain/plan.h>
#include <vector>
#include <string>
#include <memory>
#include <functional>


namespace dbprove::theorem
{
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

    /**
     * Name of Theorem
     * @param type To find name of
     * @return Short name for the Theorem
     */
    std::string_view typeName(Type type);
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
            EXPLAIN
        };

        const Type type;

        explicit Data(const Type type)
            : type(type)
        {
        }

        virtual std::string render() { return std::string{}; }
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

        std::string render() override;
    };


    class Theorem;
    class RunCtx;


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
        [[nodiscard]] std::ostream& console() const;
        [[nodiscard]] std::ostream& csv() const;

    private:
        RunCtx& state;
    };

    using TheoremFunction = std::function<void(Proof& state)>;

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
        std::string_view description;
        TheoremFunction func;
    };


    /**
     * RunContext must be passed into the Theorem prove function to provide the environment that is used for proving
     * It is also where the proofs are kept for returning to caller
     */
    class RunCtx
    {
    public:
        const sql::Engine& engine;
        const sql::Credential& credentials;
        generator::GeneratorState& generator;
        sql::ConnectionFactory factory{engine, credentials};
        std::ostream& console;
        std::ostream& csv;
        std::vector<std::unique_ptr<Proof>> proofs;

        RunCtx(
            const sql::Engine& engine,
            const sql::Credential& credentials,
            generator::GeneratorState& generator,
            std::ostream& console,
            std::ostream& csv
        )
            : engine(engine)
            , credentials(credentials)
            , generator(generator)
            , console(console)
            , csv(csv)
        {
        }

        ~RunCtx();
    };

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
