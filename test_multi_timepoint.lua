wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

-- Generate random date between 20251101 and 20251130
function get_random_date()
    local day = math.random(1, 30)
    return string.format("202511%02d", day)
end

request = function()
    local date1 = get_random_date()
    local date2 = get_random_date()
    local date3 = get_random_date()
    
    local body = string.format('{"kpiArray": ["KD9999"], "opTimeArray": ["%s", "%s", "%s"], "dimCodeArray": ["city_id", "county_id"], "dimConditionArray": [], "includeTargetData": false, "includeHistoricalData": false}', date1, date2, date3)
    return wrk.format(nil, nil, nil, body)
end
