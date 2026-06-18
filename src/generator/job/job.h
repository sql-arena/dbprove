#pragma once

#include <dbprove/generator/generator_state.h>
#include <dbprove/generator/sql_resources.h>

REGISTER_TABLE("aka_name", "job", resource::aka_name_sql, 901343, 1);
REGISTER_TABLE("aka_title", "job", resource::aka_title_sql, 361472, 1);
REGISTER_TABLE("cast_info", "job", resource::cast_info_sql, 36244344, 1);
REGISTER_TABLE("char_name", "job", resource::char_name_sql, 3140339, 1);
REGISTER_TABLE("comp_cast_type", "job", resource::comp_cast_type_sql, 4, 1);
REGISTER_TABLE("company_name", "job", resource::company_name_sql, 234997, 1);
REGISTER_TABLE("company_type", "job", resource::company_type_sql, 4, 1);
REGISTER_TABLE("complete_cast", "job", resource::complete_cast_sql, 135086, 1);
REGISTER_TABLE("info_type", "job", resource::info_type_sql, 113, 1);
REGISTER_TABLE("keyword", "job", resource::keyword_sql, 134170, 1);
REGISTER_TABLE("kind_type", "job", resource::kind_type_sql, 7, 1);
REGISTER_TABLE("link_type", "job", resource::link_type_sql, 18, 1);
REGISTER_TABLE("movie_companies", "job", resource::movie_companies_sql, 2609129, 1);
REGISTER_TABLE("movie_info", "job", resource::movie_info_sql, 14835720, 1);
REGISTER_TABLE("movie_info_idx", "job", resource::movie_info_idx_sql, 1380035, 1);
REGISTER_TABLE("movie_keyword", "job", resource::movie_keyword_sql, 4523930, 1);
REGISTER_TABLE("movie_link", "job", resource::movie_link_sql, 29997, 1);
REGISTER_TABLE("name", "job", resource::name_sql, 4167491, 1);
REGISTER_TABLE("person_info", "job", resource::person_info_sql, 2963664, 1);
REGISTER_TABLE("role_type", "job", resource::role_type_sql, 12, 1);
REGISTER_TABLE("title", "job", resource::title_sql, 2528312, 1);
