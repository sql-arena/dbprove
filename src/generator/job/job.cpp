#include <dbprove/generator/generator_state.h>

#include "dbprove_generator/embedded_sql.h"

using generator::GeneratorState;

namespace {

#define DEFINE_JOB_DOWNLOAD(TABLE) \
  void TABLE##_download(GeneratorState& state, sql::ConnectionBase*) { \
    state.prepareFileInput("job", #TABLE, ""); \
  }

DEFINE_JOB_DOWNLOAD(aka_name)
DEFINE_JOB_DOWNLOAD(aka_title)
DEFINE_JOB_DOWNLOAD(cast_info)
DEFINE_JOB_DOWNLOAD(char_name)
DEFINE_JOB_DOWNLOAD(comp_cast_type)
DEFINE_JOB_DOWNLOAD(company_name)
DEFINE_JOB_DOWNLOAD(company_type)
DEFINE_JOB_DOWNLOAD(complete_cast)
DEFINE_JOB_DOWNLOAD(info_type)
DEFINE_JOB_DOWNLOAD(keyword)
DEFINE_JOB_DOWNLOAD(kind_type)
DEFINE_JOB_DOWNLOAD(link_type)
DEFINE_JOB_DOWNLOAD(movie_companies)
DEFINE_JOB_DOWNLOAD(movie_info)
DEFINE_JOB_DOWNLOAD(movie_info_idx)
DEFINE_JOB_DOWNLOAD(movie_keyword)
DEFINE_JOB_DOWNLOAD(movie_link)
DEFINE_JOB_DOWNLOAD(name)
DEFINE_JOB_DOWNLOAD(person_info)
DEFINE_JOB_DOWNLOAD(role_type)
DEFINE_JOB_DOWNLOAD(title)

#define REGISTER_JOB_TABLE(TABLE) \
  static generator::Registrar TABLE##_registrar("job." #TABLE, "job", resource::TABLE##_sql, TABLE##_download, 0)

REGISTER_JOB_TABLE(aka_name);
REGISTER_JOB_TABLE(aka_title);
REGISTER_JOB_TABLE(cast_info);
REGISTER_JOB_TABLE(char_name);
REGISTER_JOB_TABLE(comp_cast_type);
REGISTER_JOB_TABLE(company_name);
REGISTER_JOB_TABLE(company_type);
REGISTER_JOB_TABLE(complete_cast);
REGISTER_JOB_TABLE(info_type);
REGISTER_JOB_TABLE(keyword);
REGISTER_JOB_TABLE(kind_type);
REGISTER_JOB_TABLE(link_type);
REGISTER_JOB_TABLE(movie_companies);
REGISTER_JOB_TABLE(movie_info);
REGISTER_JOB_TABLE(movie_info_idx);
REGISTER_JOB_TABLE(movie_keyword);
REGISTER_JOB_TABLE(movie_link);
REGISTER_JOB_TABLE(name);
REGISTER_JOB_TABLE(person_info);
REGISTER_JOB_TABLE(role_type);
REGISTER_JOB_TABLE(title);

#undef REGISTER_JOB_TABLE
#undef DEFINE_JOB_DOWNLOAD

} // namespace

void dbprove_force_link_job_generators() {
  (void)&aka_name_registrar;
  (void)&aka_title_registrar;
  (void)&cast_info_registrar;
  (void)&char_name_registrar;
  (void)&comp_cast_type_registrar;
  (void)&company_name_registrar;
  (void)&company_type_registrar;
  (void)&complete_cast_registrar;
  (void)&info_type_registrar;
  (void)&keyword_registrar;
  (void)&kind_type_registrar;
  (void)&link_type_registrar;
  (void)&movie_companies_registrar;
  (void)&movie_info_registrar;
  (void)&movie_info_idx_registrar;
  (void)&movie_keyword_registrar;
  (void)&movie_link_registrar;
  (void)&name_registrar;
  (void)&person_info_registrar;
  (void)&role_type_registrar;
  (void)&title_registrar;
}
