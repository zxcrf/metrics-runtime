## 工程介绍 
这是一个quarkus + JDK21的高性能 指标库API 工程，核心能力就是查询指标数据，数据库使用内存的SQLite（即配置中的metrics.engine.type=SQLite） 
之前使用MySQL作为过渡，现在已经不继续使用MySQL作为引擎了，但是MySQL中对取数SQL的拼接模式可以参考，但不一定满足SQLite引擎了。

核心输入参数为: src/main/java/com/asiainfo/metrics/model/http/KpiQueryRequest.java
核心API为/api/kpi/queryKpiData，根据用户的请求内容，返回指标数据。

## 核心表介绍
- metrics_def 指标定义表，包含了核心的定义，请参见 ./CREATED_BY_CLAUDE/BUSI_README.md
- metrics_model_def 指标模型定义表，是派生指标数据的来源宽表，参见代码中的逻辑com.asiainfo.metrics.service.KpiComputeService.buildComputeSql
- metrics_dim_def 原子维度定义表，是组合维度的构成
- metrics_comp_dim_def 组合维度定义表，指标模型、派生指标、复合指标都是基于组合维度来分组，十分重要
- modo_datasource 数据源定义表，在指标模型为最小作业单位，抽取依赖指标模型的派生指标数据时，JDBC数据源的配置

## 特别注意
### 指标持久化说明
目前只有派生指标持久化了，复合指标没有持久化，所以查询、计算复合指标时，需要递归解析至最终依赖的派生指标，只有这样才能获取到数据来源。

### 在指标的查询和聚合运算时，有一些要小心的地方
- 需要新增一个agg_func聚合函数字段（比如默认sum），因为指标根据真实业务场景，分为可加、半可加、不可加的指标，不能盲目使用sum完成维度、时间的聚合。指标的聚合函数，暂定为可加sum，半可加first_value/last_value, 不可加min/max/last_value/first_value
- 派生指标在查询时，理论上是可加的，但是由于用户指标的口径定义不专业（比如不是从DW/DWD层出指标，而是从ST层，甚至RPT层），导致半可加或不可加
- 指标表达式 kpi_expr 在派生指标中，代表取数来源，与指标模型拼接，kpi_expr在复合指标中代表查询时的计算表达式，分为以下几种情况
  - 派生指标kpi_type=extended and compute_type=normal（直接查询），kpi_expr是定义的SQL片段，不参与查询，而是使用定义的聚合函数(agg_func)用来查询，最终查询SQL如`select op_time, kpi_id, ${dimGroups}, ${agg_func}(${kpiId}) from $(kpi_id)_$(op_time)_$(组合维度编码) where op_time = ${op_time} and kpi_id = ${kpi_id} and ${dimValues}`
  - 月累计指标cycle_type=day and kpi_type=composite and compute_type=cumulative（间接查询），根据表达式累加，如KD1002的kpi_expr = KD0001,代表从opTime对应的当月1号累加到传入opTime的值, 最终查询SQL如`select op_time, '月累计指标ID' as kpi_id, ${dimGroups}, ${agg_func}(${kpiId}) from ($(kpi_id)_$(op_time1)_$(组合维度编码) union all $(kpi_id)_$(op_time2)_$(组合维度编码) union all $(kpi_id)_$(op_time...)_$(组合维度编码)) where kpi_id = ${kpi_id} and ${dimValues}` 
  - 计算指标kpi_type=composite and compute_type=expr（间接查询），根据表达式做四则运算，如KD1003的kpi_expr = KD0001/(KD0001 + KD0002),需要找到最终依赖的派生指标，最终查询SQL如`select op_time, '计算指标ID' as kpi_id, ${dimGroups}, ${agg_func}(case when kpi_id = 'KD0001' then t1.kpi_val else null end/(case when kpi_id = 'KD0001' then t1.kpi_val else null end + case when kpi_id = 'KD0002' then t2.kpi_val else null end)) from ($(KD0001)_$(op_time)_$(组合维度编码) union all $(KD0002)_$(op_time)_$(组合维度编码)) where op_time = ${op_time} and ${dimValues}`
  - 虚拟指标没有对应的元数据，需要解析。（间接查询，与计算指标十分相似），用户传入一个表达式，但这个表达式没有直接与其对应的指标元数据，如 (${KD1003}-${KD1003.lastYear})/${KD1003.lastYear},用户直接这样计算同比增长率，需要先解析表达式，分析出来需要查询KD1003当前统计周期和KD1003去年同期的值，并且做运算，是计算指标的变种，指定了特殊的op_time
- 在查询/计算开始之前，需要参考com.asiainfo.metrics.service.KpiSQLiteEngine一样，加载对应的数据表和维度表，根据用户的要求，加载历史同期/上一周期的数据，目标值数据等
- 需要考虑到当某一个批次指标数据缺失的场景，不能报错阻塞整个查询

## 目标与要求
- 补全SQLite引擎的实现，你至少可以通过打印SQL的方式完成SQLite引擎的业务验证，如果你能直接mock sqlite的数据，那就更好了。
- 使用MySQL MCP查询、增加需要的元数据
- 尽量不要修改com.asiainfo.metrics.service.KpiRDBEngine的内容，但如果com.asiainfo.metrics.service.AbstractKpiQueryEngineImpl没有抽象好，需要讲RDB引擎的内容独立出来时，才可以修改RDB引擎
- 不允许在任何的循环、递归中查询数据库，有批量查询的需求请先在内存完成SQL preparestatement + params的处理，然后在循环、递归外一次性查询