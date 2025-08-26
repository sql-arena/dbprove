#include "tpch_text.h"

#include <plog/Log.h>

#include "weighted_select.h"
std::optional<generator::WeightedSelect<std::string_view>> dist_grammar;
std::optional<generator::WeightedSelect<std::string_view>> dist_noun_phrase;
std::optional<generator::WeightedSelect<std::string_view>> dist_verb_phrase;
std::optional<generator::WeightedSelect<std::string_view>> dist_preposition;
std::optional<generator::WeightedSelect<std::string_view>> dist_termination;
std::optional<generator::WeightedSelect<std::string_view>> dist_verb;
std::optional<generator::WeightedSelect<std::string_view>> dist_noun;
std::optional<generator::WeightedSelect<std::string_view>> dist_auxiliaries;
std::optional<generator::WeightedSelect<std::string_view>> dist_adverb;
std::optional<generator::WeightedSelect<std::string_view>> dist_article;
std::optional<generator::WeightedSelect<std::string_view>> dist_adjective;
std::mutex generate_mutex;

using dist = std::pair<std::string_view, unsigned>;

// first level grammar.
// N=noun phrase, V=verb phrase, P=prepositional phrase, T=sentence termination
constexpr dist grammar[] = {
    {"NVT", 3},
    {"NVPT", 3},
    {"NVNT", 3},
    {"NPVNT", 1},
    {"NPVPT", 1}
};
// Noun phrases.
// N=noun, A=article, J=adjective, D=adverb
constexpr dist noun_phrases[] = {
    {"N", 10},
    {"JN", 20},
    {"JJN", 10},
    {"DJN", 50}
};
// Verb phrases.
// V=verb, X=auxiliary, D=adverb
constexpr dist verb_phrases[] = {
    {"V", 30},
    {"XV", 21},
    {"VD", 40},
    {"XVD", 1}
};

//  nouns
constexpr dist nouns[] = {
    {"packages", 40},
    {"requests", 40},
    {"accounts", 40},
    {"deposits", 40},
    {"foxes", 20},
    {"ideas", 20},
    {"theodolites", 20},
    {"pinto beans", 20},
    {"instructions", 20},
    {"dependencies", 10},
    {"excuses", 10},
    {"platelets", 10},
    {"asymptotes", 1},
    {"courts", 5},
    {"dolphins", 5},
    {"multipliers", 1},
    {"sauternes", 1},
    {"warthogs", 1},
    {"frets", 1},
    {"dinos", 1},
    {"attainments", 1},
    {"somas", 1},
    {"Tiresias", 1},
    {"patterns", 1},
    {"forges", 1},
    {"braids", 1},
    {"frays", 1},
    {"warhorses", 1},
    {"dugouts", 1},
    {"notornis", 1},
    {"epitaphs", 1},
    {"pearls", 1},
    {"tithes", 1},
    {"waters", 1},
    {"orbits", 1},
    {"gifts", 1},
    {"sheaves", 1},
    {"depths", 1},
    {"sentiments", 1},
    {"decoys", 1},
    {"realms", 1},
    {"pains", 1},
    {"grouches", 1},
    {"escapades", 1},
    {"hockey players", 1}

};
constexpr dist verbs[] = {
    {"sleep", 20},
    {"wake", 20},
    {"are", 20},
    {"cajole", 20},
    {"haggle", 20},
    {"nag", 10},
    {"use", 10},
    {"boost", 10},
    {"affix", 5},
    {"detect", 5},
    {"integrate", 5},
    {"maintain", 1},
    {"nod", 1},
    {"was", 1},
    {"lose", 1},
    {"sublate", 1},
    {"solve", 1},
    {"thrash", 1},
    {"promise", 1},
    {"engage", 1},
    {"hinder", 1},
    {"print", 1},
    {"x-ray", 1},
    {"breach", 1},
    {"eat", 1},
    {"grow", 1},
    {"impress", 1},
    {"mold", 1},
    {"poach", 1},
    {"serve", 1},
    {"run", 1},
    {"dazzle", 1},
    {"snooze", 1},
    {"doze", 1},
    {"unwind", 1},
    {"kindle", 1},
    {"play", 1},
    {"hang", 1},
    {"believe", 1},
    {"doubt", 1}
};
constexpr dist adjectives[] = {
    {"special", 20},
    {"pending", 20},
    {"unusual", 20},
    {"express", 20},
    {"furious", 1},
    {"sly", 1},
    {"careful", 1},
    {"blithe", 1},
    {"quick", 1},
    {"fluffy", 1},
    {"slow", 1},
    {"quiet", 1},
    {"ruthless", 1},
    {"thin", 1},
    {"close", 1},
    {"dogged", 1},
    {"daring", 1},
    {"brave", 1},
    {"stealthy", 1},
    {"permanent", 1},
    {"enticing", 1},
    {"idle", 1},
    {"busy", 1},
    {"regular", 50},
    {"final", 40},
    {"ironic", 40},
    {"even", 30},
    {"bold", 20},
    {"silent", 10}
};

constexpr dist adverbs[] = {
    {"sometimes", 1},
    {"always", 1},
    {"never", 1},
    {"furiously", 50},
    {"slyly", 50},
    {"carefully", 50},
    {"blithely", 40},
    {"quickly", 30},
    {"fluffily", 20},
    {"slowly", 1},
    {"quietly", 1},
    {"ruthlessly", 1},
    {"thinly", 1},
    {"closely", 1},
    {"doggedly", 1},
    {"daringly", 1},
    {"bravely", 1},
    {"stealthily", 1},
    {"permanently", 1},
    {"enticingly", 1},
    {"idly", 1},
    {"busily", 1},
    {"regularly", 1},
    {"finally", 1},
    {"ironically", 1},
    {"evenly", 1},
    {"boldly", 1},
    {"silently", 1}
};

constexpr dist articles[] = {
    {"the", 50},
    {"a", 20},
    {"an", 5}
};

constexpr dist prepositions[] = {
    {"about", 50},
    {"above", 50},
    {"according to", 50},
    {"across", 50},
    {"after", 50},
    {"against", 40},
    {"along", 40},
    {"alongside of", 30},
    {"among", 30},
    {"around", 20},
    {"at", 107},
    {"atop", 1},
    {"before", 1},
    {"behind", 1},
    {"beneath", 1},
    {"beside", 1},
    {"besides", 1},
    {"between", 1},
    {"beyond", 1},
    {"by", 1},
    {"despite", 1},
    {"during", 1},
    {"except", 1},
    {"for", 1},
    {"from", 1},
    {"in place of", 1},
    {"inside", 1},
    {"instead of", 1},
    {"into", 1},
    {"near", 1},
    {"of", 1},
    {"on", 1},
    {"outside", 1},
    {"over", 1},
    {"past", 1},
    {"since", 1},
    {"through", 1},
    {"throughout", 1},
    {"to", 1},
    {"toward", 1},
    {"under", 1},
    {"until", 1},
    {"up", 1},
    {"upon", 1},
    {"without", 1},
    {"with", 1},
    {"within", 1}
};
constexpr dist auxiliaries[] = {
    {"may", 1},
    {"might", 1},
    {"shall", 1},
    {"will", 1},
    {"would", 1},
    {"can", 1},
    {"could", 1},
    {"should", 1},
    {"ought to", 1},
    {"must", 1},
    {"will have to", 1},
    {"shall have to", 1},
    {"could have to", 1},
    {"should have to", 1},
    {"must have to", 1},
    {"need to", 1},
    {"try to", 1}
};

constexpr dist terminators[] = {
    {".", 50},
    {";", 1},
    {":", 1},
    {"?", 1},
    {"!", 1},
    {"--", 1},
};


std::string generator::TpchText::generatedText_;

void generator::TpchText::addWord(const std::string_view word) {
  generatedText_.append(" ");
  generatedText_.append(word);
}

void generator::TpchText::generateNounPhrase() {
  for (const char c : dist_noun_phrase->next()) {
    switch (c) {
      case 'N':
        addWord(dist_noun->next());
        break;
      case 'A':
        addWord(dist_article->next());
        break;
      case 'J':
        addWord(dist_adjective->next());
        break;
      case 'D':
        addWord(dist_adverb->next());
        break;
      default: ;
    }
  }
}

void generator::TpchText::generateVerbPhrase() {
  for (const char c : dist_verb_phrase->next()) {
    switch (c) {
      case 'V':
        addWord(dist_verb->next());
        break;
      case 'X':
        addWord(dist_auxiliaries->next());
        break;
      case 'D':
        addWord(dist_adverb->next());
        break;
      default: ;
    }
  }
}

void generator::TpchText::generatePrepositionalPhrase() {
  addWord(dist_preposition->next());
  addWord("the");
  generateNounPhrase();
}

void generator::TpchText::generate() {
  std::lock_guard lock(generate_mutex);
  if (!generatedText_.empty()) {
    return;
  }
  PLOGI << "TPC-H input text buffer generation - this can take a short while...";
  dist_grammar.emplace(WeightedSelect<std::string_view>(grammar));
  dist_noun_phrase.emplace(WeightedSelect<std::string_view>(noun_phrases));
  dist_verb_phrase.emplace(WeightedSelect<std::string_view>(verb_phrases));
  dist_preposition.emplace(WeightedSelect<std::string_view>(prepositions));
  dist_termination.emplace(WeightedSelect<std::string_view>(terminators));
  dist_verb.emplace(WeightedSelect<std::string_view>(verbs));
  dist_noun.emplace(WeightedSelect<std::string_view>(nouns));
  dist_auxiliaries.emplace(WeightedSelect<std::string_view>(auxiliaries));
  dist_adverb.emplace(WeightedSelect<std::string_view>(adverbs));
  dist_article.emplace(WeightedSelect<std::string_view>(articles));
  dist_adjective.emplace(WeightedSelect<std::string_view>(adjectives));

  // Reserve a bit more than we need, so we can overshoot safely
  generatedText_.reserve(max_generated_text_length + 1000);
  while (generatedText_.length() < max_generated_text_length) {
    std::string_view current_grammar = dist_grammar->next();
    for (const char c : current_grammar) {
      switch (c) {
        case 'N':
          generateNounPhrase();
          break;
        case 'V':
          generateVerbPhrase();
          break;
        case 'P':
          generatePrepositionalPhrase();
          break;
        case 'T':
          generatedText_.append(dist_termination->next());
          break;
        default: ;
      }
    }
  }

  for (size_t o = 0; o < max_generated_text_length; ++o) {
    assert(generatedText_.c_str()[o] != 0);
  }

  PLOGI << "TPC-H input text buffer done!";
}

std::string generator::TpchText::next() {
  const auto len = lengthDistribution_(gen_);

  const size_t offset = offsetDistribution_.next(max_generated_text_length - len);
  return generatedText_.substr(offset, len);
}