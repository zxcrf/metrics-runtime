### 指标标准API返回，与第三方约定
默认查询历史数据，但是不开启目标值查询
```json
{
  "msg": "成功！指标数据查询耗时：85ms",
  "data": [
    {
      "dataArray": [
        {
          "kpiValues": {
            "ATO251024ITR1": {
              "current": "127.7",
              "lastYear": "0.0", 
              "lastCycle": "0.0"
            },
            "ATO2510244XLB": {
              "current": "237.3",
              "lastYear": "0.0",
              "lastCycle": "0.0"
            }
          },
          "county_id": "4",
          "region_id_desc": "朝阳分公司-亚运经营网格",
          "opTime": "20251023",
          "county_id_desc": "朝阳分公司",
          "region_id": "904022"
        },
        {
          "kpiValues": {
            "ATO251024ITR1": {
              "current": "0.0",
              "lastYear": "0.0",
              "lastCycle": "133.1"
            },
            "ATO2510244XLB": {
              "current": "0.0",
              "lastYear": "0.0",
              "lastCycle": "244.6"
            }
          },
          "county_id": "4",
          "region_id_desc": "朝阳分公司-南磨房经营网格",
          "opTime": "20251022",
          "county_id_desc": "朝阳分公司",
          "region_id": "904020"
        }
      ]
    }
  ],
  "status": "0000"
}
```

### 报表查询返回，与第三方约定
默认不查询历史数据，但是开启目标值查询
```json
{
  "hasNext": false,
  "data": [
    {
      "region_id_desc": "平谷分公司-东部经营网格",
      "${KD1002}_checkResult": "--",
      "region_id": "910004",
      "${KD1002}_checkDesc": "--",
      "${KD1009}_checkResult": "--",
      "city_id_desc": "北京市",
      "${KD1002}_targetValue": "--",
      "${KD1008}_checkResult": "通过测试",
      "${KD1008}": "977.2",
      "county_id": "10",
      "${KD1009}": "1086.7",
      "${KD1008}_targetValue": "1000",
      "${KD1009}_checkDesc": "--",
      "opTime": "20251106",
      "county_id_desc": "平谷分公司",
      "${KD1009}_targetValue": "--",
      "${KD1008}_checkDesc": "检查描述",
      "${KD1002}": "1082.57",
      "city_id": "999"
    },
    {
      "region_id_desc": "平谷分公司-东部经营网格",
      "${KD1002}_checkResult": "--",
      "region_id": "910004",
      "${KD1002}_checkDesc": "--",
      "${KD1009}_checkResult": "--",
      "city_id_desc": "北京市",
      "${KD1002}_targetValue": "--",
      "${KD1008}_checkResult": "--",
      "${KD1008}": "0",
      "county_id": "10",
      "${KD1009}": "0",
      "${KD1008}_targetValue": "--",
      "${KD1009}_checkDesc": "--",
      "opTime": "20251006",
      "county_id_desc": "平谷分公司",
      "${KD1009}_targetValue": "--",
      "${KD1008}_checkDesc": "--",
      "${KD1002}": "0",
      "city_id": "999"
    },
    {
      "region_id_desc": "平谷分公司-东部经营网格",
      "${KD1002}_checkResult": "--",
      "region_id": "910004",
      "${KD1002}_checkDesc": "--",
      "${KD1009}_checkResult": "--",
      "city_id_desc": "北京市",
      "${KD1002}_targetValue": "--",
      "${KD1008}_checkResult": "--",
      "${KD1008}": "0",
      "county_id": "10",
      "${KD1009}": "0",
      "${KD1008}_targetValue": "--",
      "${KD1009}_checkDesc": "--",
      "opTime": "20241106",
      "county_id_desc": "平谷分公司",
      "${KD1009}_targetValue": "--",
      "${KD1008}_checkDesc": "--",
      "${KD1002}": "0",
      "city_id": "999"
    },
    {
      "region_id_desc": "平谷分公司-西部经营网格",
      "${KD1002}_checkResult": "--",
      "region_id": "910005",
      "${KD1002}_checkDesc": "--",
      "${KD1009}_checkResult": "--",
      "city_id_desc": "北京市",
      "${KD1002}_targetValue": "--",
      "${KD1008}_checkResult": "--",
      "${KD1008}": "0",
      "county_id": "10",
      "${KD1009}": "0",
      "${KD1008}_targetValue": "--",
      "${KD1009}_checkDesc": "--",
      "opTime": "20251006",
      "county_id_desc": "平谷分公司",
      "${KD1009}_targetValue": "--",
      "${KD1008}_checkDesc": "--",
      "${KD1002}": "0",
      "city_id": "999"
    },
    {
      "region_id_desc": "平谷分公司-南部经营网格",
      "${KD1002}_checkResult": "--",
      "region_id": "910002",
      "${KD1002}_checkDesc": "--",
      "${KD1009}_checkResult": "--",
      "city_id_desc": "北京市",
      "${KD1002}_targetValue": "--",
      "${KD1008}_checkResult": "--",
      "${KD1008}": "--",
      "county_id": "10",
      "${KD1009}": "--",
      "${KD1008}_targetValue": "--",
      "${KD1009}_checkDesc": "--",
      "opTime": "20241106",
      "county_id_desc": "平谷分公司",
      "${KD1009}_targetValue": "--",
      "${KD1008}_checkDesc": "--",
      "${KD1002}": "0",
      "city_id": "999"
    },
    {
      "region_id_desc": "平谷分公司-西部经营网格",
      "${KD1002}_checkResult": "--",
      "region_id": "910005",
      "${KD1002}_checkDesc": "--",
      "${KD1009}_checkResult": "--",
      "city_id_desc": "北京市",
      "${KD1002}_targetValue": "--",
      "${KD1008}_checkResult": "--",
      "${KD1008}": "--",
      "county_id": "10",
      "${KD1009}": "--",
      "${KD1008}_targetValue": "--",
      "${KD1009}_checkDesc": "--",
      "opTime": "20241106",
      "county_id_desc": "平谷分公司",
      "${KD1009}_targetValue": "--",
      "${KD1008}_checkDesc": "--",
      "${KD1002}": "0",
      "city_id": "999"
    },
    {
      "region_id_desc": "平谷分公司-南部经营网格",
      "${KD1002}_checkResult": "--",
      "region_id": "910002",
      "${KD1002}_checkDesc": "--",
      "${KD1009}_checkResult": "--",
      "city_id_desc": "北京市",
      "${KD1002}_targetValue": "--",
      "${KD1008}_checkResult": "--",
      "${KD1008}": "--",
      "county_id": "10",
      "${KD1009}": "--",
      "${KD1008}_targetValue": "--",
      "${KD1009}_checkDesc": "--",
      "opTime": "20251006",
      "county_id_desc": "平谷分公司",
      "${KD1009}_targetValue": "--",
      "${KD1008}_checkDesc": "--",
      "${KD1002}": "0",
      "city_id": "999"
    },
    {
      "region_id_desc": "平谷分公司-南部经营网格",
      "${KD1002}_checkResult": "--",
      "region_id": "910002",
      "${KD1002}_checkDesc": "--",
      "${KD1009}_checkResult": "--",
      "city_id_desc": "北京市",
      "${KD1002}_targetValue": "--",
      "${KD1008}_checkResult": "--",
      "${KD1008}": "--",
      "county_id": "10",
      "${KD1009}": "--",
      "${KD1008}_targetValue": "--",
      "${KD1009}_checkDesc": "--",
      "opTime": "20251106",
      "county_id_desc": "平谷分公司",
      "${KD1009}_targetValue": "--",
      "${KD1008}_checkDesc": "--",
      "${KD1002}": "1067.07",
      "city_id": "999"
    },
    {
      "region_id_desc": "平谷分公司-西部经营网格",
      "${KD1002}_checkResult": "--",
      "region_id": "910005",
      "${KD1002}_checkDesc": "--",
      "${KD1009}_checkResult": "--",
      "city_id_desc": "北京市",
      "${KD1002}_targetValue": "--",
      "${KD1008}_checkResult": "--",
      "${KD1008}": "--",
      "county_id": "10",
      "${KD1009}": "--",
      "${KD1008}_targetValue": "--",
      "${KD1009}_checkDesc": "--",
      "opTime": "20251106",
      "county_id_desc": "平谷分公司",
      "${KD1009}_targetValue": "--",
      "${KD1008}_checkDesc": "--",
      "${KD1002}": "1258.09",
      "city_id": "999"
    }
  ],
  "total": 9,
  "status": "0000",
  "schema": [
    "region_id_desc",
    "${KD1002}_checkResult",
    "region_id",
    "${KD1002}_checkDesc",
    "${KD1009}_checkResult",
    "city_id_desc",
    "${KD1002}_targetValue",
    "${KD1008}_checkResult",
    "county_id",
    "${KD1008}_targetValue",
    "${KD1009}_checkDesc",
    "opTime",
    "county_id_desc",
    "${KD1009}_targetValue",
    "${KD1008}_checkDesc",
    "city_id"
  ]
}
```

### 指标看板查询API返回，与第三方约定
默认开启历史数据查询，不开启目标值查询
```json
{
  "msg": "成功！耗时：3ms",
  "data": [
    {
      "current": "27480", //当前值
      "unit": "万元", //指标单位
      "lastCyclePercent": 0.1705, // 环比 
      "lastYear": "240.18", //去年同期值
      "kpiId": "MCD001L00001", //指标编码 
      "lastYearPercent": 2.4018, // 同比
      "lastCycle": "17.05", // 环比值
      "kpiName": "日电信业务收入", //指标名称
      "tag": "income", //指标标签
      "status": "hot", // 当前状态
      "opTime": "20251022", //统计周期
      "viewMode": "pie", // 单击展开后的图行类别 pie | line
      "indicatorId": " uuid(32) ", // 指标的id
      "indicatorType": "extended（派生指标）/computed（复合指标）", // 指标的类型 
      "latestValues": {"20251021":"26380", "20251020":"25380", ....} //暂定最近10个批次的值，用于卡片单击展开后数据的渲染
    },.....
  ],
  "status": "0000"
}
```

### 指标卡API返回，与第三方约定
查询单一指标数据，和看板类似，但是包含目标值查询
```json
参考指标看板返回的数据结构，但是包含目标值
```