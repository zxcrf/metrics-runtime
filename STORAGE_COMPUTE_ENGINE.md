### 引擎类型
目前支持MySQL与SQLite，但是SQLite几乎没有实现，我会在下方说明
当前引擎用${CURRENT_ENGINE}代替


### 计算方案
1. 连接第三方库抽取派生指标数据:
根据数据库的metrics_model_def，作为指标取数模型，拼接所有派生指标的取数SQL片段，如
```sql
select xxx as op_time, -- 时间维度
       ${dimValues} ,  -- 其他维度，通过组合维度编码获取
       ${metrics_def.kpi_expr1} as ${metrics_def.kpi_id1}, -- 派生指标的表达式
       ${metrics_def.kpi_expr2} as ${metrics_def.kpi_id2}, -- 派生指标的表达式
       ${metrics_def.kpi_expr3} as ${metrics_def.kpi_id3}, -- 派生指标的表达式
       ....
from (
    ${metrics_model_def.model_sql}
    )
group by op_time, ${dimValues}
```

2. 将横表指标结果，纵表化拆分、存储
当${CURRENT_ENGINE}是MySQL时，数据表参考 `BUSI_README.md`文件，将指标数据入库，需要保证幂等性
当${CURRENT_ENGINE}是SQLite时，每一个指标数据文件参考 `BUSI_README.md`文件，输出为SQLite.db文件，压缩后上传至MinIO

### 查询方案
目前实现的MySQL勉强能用了，但是需要立刻支持SQLite模式，生产环境着急上线。所以SQL要适配SQLite attach dbfile后的模式，
每一个指标，每一个批次，都是一个单独的表，关联维度时也一样。

考虑到SQLite最多attach 10个文件的限制，需要每attach一个文件，将文件中的表copy到当前库，然后detach掉，直到加载完成所有需要的表，再拼SQL查询、计算。

所以两个存算引擎拼接SQL的逻辑需要尽量通用