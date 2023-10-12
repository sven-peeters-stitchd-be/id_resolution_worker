# Purpose

This repo contains the code that will simulate a CDP alike identity resolution system.
Using wrangler and a free version of CloudFlare workers, this is almost a solution free of use.
Only the backend BigQuery storage is not really free.

Bigquery table DDL's

[{
  "table_name": "id_types",
  "ddl": "CREATE TABLE `??.??.id_types`\n(\n  id_type_id INT64,\n  id_type_name STRING,\n  id_type_cookie_name STRING\n)\nOPTIONS(\n  default_rounding_mode\u003d\"ROUND_HALF_AWAY_FROM_ZERO\"\n);"
}, {
  "table_name": "hard_id_list",
  "ddl": "CREATE TABLE `??.??.hard_id_list`\n(\n  hard_id_list_id STRING(36),\n  email STRING\n)\nOPTIONS(\n  default_rounding_mode\u003d\"ROUND_HALF_AWAY_FROM_ZERO\"\n);"
}, {
  "table_name": "soft_id_list",
  "ddl": "CREATE TABLE `??.??.soft_id_list`\n(\n  soft_id_list_id STRING(36),\n  id_type INT64,\n  id_value STRING,\n  email STRING,\n  hard_id_list_id STRING(36)\n)\nOPTIONS(\n  default_rounding_mode\u003d\"ROUND_HALF_AWAY_FROM_ZERO\"\n);"
}]

Since most npm packages are not supported on CloudFlare Workers, it's using the getTokenFromGCPServiceAccount method from '@sagi.io/workers-jwt' to get the access token, then uses the BigQuery REST API to do all the queries.

Besided the regular settings, you will require following secrets/encrypted settings :
BQ_PRIVATEKEY_ID = ''
BQ_PRIVATEKEY_CONTENT = ''
BQ_CLIENT_EMAIL = ''
BQ_CLIENT_ID = ''
BQ_CLIENT_X509_URI = ''