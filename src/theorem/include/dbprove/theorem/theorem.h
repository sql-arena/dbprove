#pragma once
#include <vector>
#include <string>
#include <map>
#include <memory>
#include <functional>
#include <dbprove/generator/generator_state.h>
#include <dbprove/sql/sql.h>
#include <dbprove/sql/explain/plan.h>


namespace dbprove::theorem
{
    enum class Type
    {
        CLI,
        EE,
        SE,
        PLAN,
        WLM,
        UNKNOWN
    };

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


class Theorem;
class RunState;


    class Proof
    {
    public:
        Proof(const Theorem& theorem, RunState& parent)
            : theorem(theorem)
            , state(parent)
        {
        }

        ~Proof();
        const Theorem& theorem;
        std::vector<std::unique_ptr<Data>> data;
        sql::ConnectionFactory& factory() const;
        Proof& ensure(const std::string& table);
        /**
         * Make sure the schema exists
         * @param schema To create if not there
         * @return
         */
        Proof& ensureSchema(const std::string& schema);
        std::ostream& console() const;
        std::ostream& csv() const;

    private:
        RunState& state;
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
            , theorem(std::move(theorem))
            , description(std::move(description))
            , func(func)
        {
        }

        Type type;
        std::string theorem;
        std::string_view description;
        TheoremFunction func;
    };


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

    class RunState
    {
    public:
        const sql::Engine& engine;
        const sql::Credential& credentials;
        generator::GeneratorState& generator;
        sql::ConnectionFactory factory{engine, credentials};
        std::ostream& console;
        std::ostream& csv;
        std::vector<std::unique_ptr<Proof>> proofs;

        RunState(
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

        ~RunState();
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
     * @param input_state Caller supplied state with all the info neded to run
     */
    void prove(const std::vector<const Theorem*>& theorems, RunState& input_state);
}
