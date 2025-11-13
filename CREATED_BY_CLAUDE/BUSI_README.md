## 指标定义
KD1001 日指标
KC1002 月累计指标
KM1003 月指标
KY1004 年累计指标
第一位代表派生指标和复合指标，第二位统计周期，第三位代表业务主题，4-6位顺序

### 原子指标
A101 ，第一位A固定，第二位代表业务主题，为数字，3-4位顺序

## 指标计算定义：
### 指标计算元数据定义
- 指标类型(kpi_type) 
  派生指标extended
  计算指标computed

- 计算类型(compute_method)
  普通计算normal
  表达式计算expr
  累计cumulative(暂时不实现)

- 指标表达式(kpi_expr)
  SQL片段，如sum(KH0001)
  一个指标编码，如KD1002
  一个含有若干指标和时间点的四则运算表达式,如KD1002.lastCycle/KM1001

## 指标计算约定：
派生指标的计算类型固定为 normal， 直接在指标模型上，拼接指标表达式作为SQL片段，如
select op_time, ${dimGroup}, sum(KH0001) as KH0001, KH0003 as KH0003  -- 指标表达式 as 指标编码
from (指标模型)
group by op_time, ${dimGroup}
派生指标的**计算**是指标生产者的工作，和本工程（指标API服务）无关，本工程只关注数据查询。
数据查询时默认使用sum(kpi_val)的方式，从数据表中获取派生指标的值

计算指标有两种计算类型
计算类型 = expr时（指标表达式计算）
通过解析指标表达式，最终递归解析至依赖的派生指标数据表，以SQL嵌套的方式完成表达式值的计算

（暂不实现）计算类型 = cumulative时，要求只能填写一个派生指标的编码
根据依赖的派生指标，找到对应的数据表，SQL完成对应指标值的累加


## 指标数据SQLite文件规范
1. 数据文件说明
   /bucketName/kpi_id/op_time/compDimCode/file.db
   文件名称 $(kpi_id)_$(op_time)_$(组合维度编码).db 如DCD005L00081_20251022_CD005.db
   表名为kpi_${kpi_id}_${op_time}_${组合维度编码}，数据与下方数据库中要求一致

2. 维度文件说明
   dim_code/ dim_value / dim_id/ parent_dim_code
   表名为kpi_dim_${now}_${组合维度编码}，数据与下方数据库中要求一致。表名增加${now}是需要保证每天的维度值都是最新的，关联时使用最新维度


## 指标数据库表规范
包含指标数据和维度数据，
- 数据表名为kpi_day_CD003，命名约定是kpi_周期_组合维度编码
- 维表名为kpi_dim_CD003，命名约定是kpi_dim_组合维度编码
- （新增）指标目标值表名kpi_target_value_CD003，命名约定是kpi_target_value_组合维度编码
  建表语句要求/表结构设计
  CREATE TABLE `kpi_day_CD003` (  -- 遵循命名约定
  `kpi_id` varchar(32) DEFAULT NULL,  -- 约定字段名，必须是kpi_id
  `op_time` varchar(16) DEFAULT NULL, -- 约定字段名，必须是op_time
  `city_id` varchar(3) CHARACTER SET utf8mb4 NOT NULL DEFAULT '', -- 组合维度CD003中对应的具体原子维度
  `county_id` varchar(16) DEFAULT NULL, -- CD003的原子维度
  `region_id` varchar(32) DEFAULT NULL, -- CD003的原子维度
  `kpi_val` varchar(32) DEFAULT NULL,    -- 约定字段名，必须是kpi_val
  KEY `idx_kpi_day_CD003` (`op_time`,`kpi_id`)  -- 必建索引，按照要求的顺序
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `kpi_dim_CD003` ( -- 每个维度表的表结构都是一样的
`dim_code` varchar(32) DEFAULT NULL, -- 维度的key
`dim_val` varchar(128) DEFAULT NULL, -- 维度的value
`dim_id` varchar(32) DEFAULT NULL,   -- 维度的db_col_name
`parent_dim_code` varchar(32) DEFAULT NULL  -- 当前维度的parent_key
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `kpi_target_value_CD003` (
`op_time` varchar(32) DEFAULT NULL,       -- 统计周期
`kpi_id` varchar(32) DEFAULT NULL,
`city_id` varchar(32) DEFAULT NULL,
`county_id` varchar(32) DEFAULT NULL,
`region_id` varchar(32) DEFAULT NULL,
`target_value` varchar(32) DEFAULT NULL,  -- 目标值
`check_result` varchar(64) DEFAULT NULL,  -- 检查结果
`check_desc` varchar(512) DEFAULT NULL,   -- 对应的检查描述
`eff_start_date` datetime DEFAULT NULL,   -- 规则开始时间
`eff_end_date` datetime DEFAULT NULL      -- 规则结束时间
) ENGINE=InnoDB DEFAULT CHARSET=utf8
指标数据表数据参考如下：
请使用MySQL MCP查看

维度表数据参考如下：
请使用MySQL MCP查看
