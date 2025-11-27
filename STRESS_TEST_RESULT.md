## 老MySQL引擎的测试结果
wrk -t12 -c100 -d30s -s post.lua --latency http://localhost:8080/api/kpi/queryKpiData 
Running 30s test @ http://localhost:8080/api/kpi/queryKpiData
  12 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    76.83ms  165.98ms   1.91s    95.17%
    Req/Sec   190.43     78.84   434.00     61.06%
  Latency Distribution
     50%   37.32ms
     75%   56.98ms
     90%  103.58ms
     99%    1.02s 
  65264 requests in 30.08s, 11.45MB read
Requests/sec:   2169.62
Transfer/sec:    389.91KB

## 新的SQLite调整过很多次的测试结果
wrk -t12 -c100 -d30s -s post.lua --latency http://localhost:8080/api/v2/kpi/queryKpiData
Running 30s test @ http://localhost:8080/api/v2/kpi/queryKpiData
  12 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   576.20ms  374.68ms   1.99s    67.37%
    Req/Sec    16.53     12.18   120.00     67.70%
  Latency Distribution
     50%  521.93ms
     75%  789.17ms
     90%    1.10s 
     99%    1.72s 
  4904 requests in 30.06s, 206.14MB read
  Socket errors: connect 0, read 0, write 0, timeout 53
Requests/sec:    163.12
Transfer/sec:      6.86MB