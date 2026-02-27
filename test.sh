source env.sh

#curl -sS -X GET "https://$DATABRICKS_HOSTNAME/api/2.0/sql/history/queries" \
#  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
#  -H "Content-Type: application/json" \
#  -d '{
#    "max_results": 1,
#    "filter_by": {
#      "statement_ids": ["'01f113df-636f-15b3-9931-18859c2686bd'"]
#    }
#  }' | jq


  curl -sS -X GET "https://$DATABRICKS_HOSTNAME/api/2.0/sql/history/queries" \
    -H "Authorization: Bearer $DATABRICKS_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"max_results" : 10, "filter_by": {"statement_ids" : ["01f113e5-de64-1755-a02d-233b63c2c4eb"]}}' \
   | jq
