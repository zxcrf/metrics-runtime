#!/bin/bash

# 测试KPI查询API

curl -L 'localhost:8080/api/kpi/queryKpiData' \
-H 'Content-Type: application/json' \
-d '{
    "dimCodeArray": [
        "county_id"
    ],
    "opTimeArray": [
        "20251101"
    ],
    "kpiArray": [
        "KD1002","${KD1002}"
    ],
    "dimConditionArray": [
        {
            "dimConditionCode": "county_id",
            "dimConditionVal": "4,10"
        }
    ],
    "sortOptions": {
        "county_id": "asc",
        "KD1008": "desc"
    }
}' \
--max-time 30
