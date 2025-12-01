package com.asiainfo.metrics.v2.domain.model;

import java.util.Collections;
import java.util.List;

public record SqlRequest(String sql, List<Object> params) {
    public SqlRequest(String sql) {
        this(sql, Collections.emptyList());
    }
}
