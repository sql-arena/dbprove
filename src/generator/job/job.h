#pragma once

#include <dbprove/generator/generator_state.h>
#include <dbprove/generator/sql_resources.h>

REGISTER_TABLE("aka_name", "job", resource::aka_name_sql, 0, 1);
REGISTER_TABLE("aka_title", "job", resource::aka_title_sql, 0, 1);
REGISTER_TABLE("cast_info", "job", resource::cast_info_sql, 0, 1);
REGISTER_TABLE("char_name", "job", resource::char_name_sql, 0, 1);
REGISTER_TABLE("comp_cast_type", "job", resource::comp_cast_type_sql, 0, 1);
REGISTER_TABLE("company_name", "job", resource::company_name_sql, 0, 1);
REGISTER_TABLE("company_type", "job", resource::company_type_sql, 0, 1);
REGISTER_TABLE("complete_cast", "job", resource::complete_cast_sql, 0, 1);
REGISTER_TABLE("info_type", "job", resource::info_type_sql, 0, 1);
REGISTER_TABLE("keyword", "job", resource::keyword_sql, 0, 1);
REGISTER_TABLE("kind_type", "job", resource::kind_type_sql, 0, 1);
REGISTER_TABLE("link_type", "job", resource::link_type_sql, 0, 1);
REGISTER_TABLE("movie_companies", "job", resource::movie_companies_sql, 0, 1);
REGISTER_TABLE("movie_info", "job", resource::movie_info_sql, 0, 1);
REGISTER_TABLE("movie_info_idx", "job", resource::movie_info_idx_sql, 0, 1);
REGISTER_TABLE("movie_keyword", "job", resource::movie_keyword_sql, 0, 1);
REGISTER_TABLE("movie_link", "job", resource::movie_link_sql, 0, 1);
REGISTER_TABLE("name", "job", resource::name_sql, 0, 1);
REGISTER_TABLE("person_info", "job", resource::person_info_sql, 0, 1);
REGISTER_TABLE("role_type", "job", resource::role_type_sql, 0, 1);
REGISTER_TABLE("title", "job", resource::title_sql, 0, 1);
