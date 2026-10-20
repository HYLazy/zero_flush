# perf_v2 analysis

Source: `/home/embed/hyl/metadata_offload/bench/out/perf_v2`. Service throughput only (measurement cohort); invalid runs excluded. Latency percentiles are bucket **lower bounds** from the cohort per-class histograms (hist_version 2); the bucket width is given where it matters so the resolution is visible.

## M4 (108 runs, 0 invalid/excluded)

### M4 arms (L0 geometry at a fixed working set)

| rid | backlog_files | backlog_overlap | ops_s | get_hit_n | get_hit_p99_us | get_miss_n | get_miss_p99_us | censored | valid |
|---|---|---|---|---|---|---|---|---|---|
| m4_k16_narrow_frozen_r1_build | 16 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_narrow_frozen_r1_read_deep | - | - | 1707278.19 | 102436690.00 | 14.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_frozen_r1_read_recent | - | - | 1918187.43 | 115091244.00 | 7.7 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_frozen_r2_build | 16 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_narrow_frozen_r2_read_deep | - | - | 1682368.41 | 100942102.00 | 14.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_frozen_r2_read_recent | - | - | 1877206.52 | 112632391.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_frozen_r3_build | 16 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_narrow_frozen_r3_read_deep | - | - | 1681311.67 | 100878700.00 | 14.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_frozen_r3_read_recent | - | - | 1888542.35 | 113312541.00 | 7.7 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_online_r1_build | 16 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_narrow_online_r1_read_deep | - | - | 1689419.11 | 101365147.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_online_r1_read_recent | - | - | 1910280.96 | 114616858.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_online_r2_build | 16 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_narrow_online_r2_read_deep | - | - | 1729374.71 | 103762481.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_online_r2_read_recent | - | - | 1908854.78 | 114531287.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_online_r3_build | 16 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_narrow_online_r3_read_deep | - | - | 1745503.72 | 104730223.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_narrow_online_r3_read_recent | - | - | 1930495.52 | 115829731.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_frozen_r1_build | 16 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_wide_frozen_r1_read_deep | - | - | 1675117.11 | 100507024.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_frozen_r1_read_recent | - | - | 1871041.02 | 112262461.00 | 9.2 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_frozen_r2_build | 16 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_wide_frozen_r2_read_deep | - | - | 1714221.53 | 102853292.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_frozen_r2_read_recent | - | - | 1850873.78 | 111052427.00 | 9.2 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_frozen_r3_build | 16 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_wide_frozen_r3_read_deep | - | - | 1688020.28 | 101281217.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_frozen_r3_read_recent | - | - | 1904115.40 | 114246923.00 | 9.2 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_online_r1_build | 16 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_wide_online_r1_read_deep | - | - | 1724336.85 | 103460211.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_online_r1_read_recent | - | - | 1916427.78 | 114985667.00 | 8.2 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_online_r2_build | 16 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_wide_online_r2_read_deep | - | - | 1702728.48 | 102163709.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_online_r2_read_recent | - | - | 1934164.84 | 116049889.00 | 8.2 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_online_r3_build | 16 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k16_wide_online_r3_read_deep | - | - | 1716544.18 | 102992651.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k16_wide_online_r3_read_recent | - | - | 1915559.97 | 114933598.00 | 8.2 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_frozen_r1_build | 1 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_narrow_frozen_r1_read_deep | - | - | 1711495.95 | 102689756.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_frozen_r1_read_recent | - | - | 1915376.02 | 114922561.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_frozen_r2_build | 1 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_narrow_frozen_r2_read_deep | - | - | 1712919.37 | 102775162.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_frozen_r2_read_recent | - | - | 1909916.64 | 114594999.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_frozen_r3_build | 1 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_narrow_frozen_r3_read_deep | - | - | 1724748.87 | 103484932.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_frozen_r3_read_recent | - | - | 1906200.71 | 114372042.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_online_r1_build | 1 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_narrow_online_r1_read_deep | - | - | 1741824.58 | 104509475.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_online_r1_read_recent | - | - | 1930816.37 | 115848982.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_online_r2_build | 1 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_narrow_online_r2_read_deep | - | - | 1763356.80 | 105801408.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_online_r2_read_recent | - | - | 1919895.72 | 115193743.00 | 7.7 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_online_r3_build | 1 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_narrow_online_r3_read_deep | - | - | 1731441.58 | 103886494.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_narrow_online_r3_read_recent | - | - | 1893943.12 | 113636587.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_frozen_r1_build | 1 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_wide_frozen_r1_read_deep | - | - | 1716606.49 | 102996388.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_frozen_r1_read_recent | - | - | 1917709.17 | 115062550.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_frozen_r2_build | 1 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_wide_frozen_r2_read_deep | - | - | 1714824.44 | 102889467.00 | 14.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_frozen_r2_read_recent | - | - | 1906558.20 | 114393492.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_frozen_r3_build | 1 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_wide_frozen_r3_read_deep | - | - | 1708495.43 | 102509726.00 | 14.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_frozen_r3_read_recent | - | - | 1907590.07 | 114455404.00 | 7.7 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_online_r1_build | 1 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_wide_online_r1_read_deep | - | - | 1740330.65 | 104419837.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_online_r1_read_recent | - | - | 1919719.21 | 115183152.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_online_r2_build | 1 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_wide_online_r2_read_deep | - | - | 1720314.32 | 103218858.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_online_r2_read_recent | - | - | 1893981.86 | 113638911.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_online_r3_build | 1 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k1_wide_online_r3_read_deep | - | - | 1750680.54 | 105040832.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k1_wide_online_r3_read_recent | - | - | 1880370.80 | 112822248.00 | 7.7 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_frozen_r1_build | 8 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_narrow_frozen_r1_read_deep | - | - | 1702351.55 | 102141093.00 | 14.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_frozen_r1_read_recent | - | - | 1906387.30 | 114383237.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_frozen_r2_build | 8 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_narrow_frozen_r2_read_deep | - | - | 1692835.72 | 101570142.00 | 14.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_frozen_r2_read_recent | - | - | 1930076.69 | 115804598.00 | 7.7 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_frozen_r3_build | 8 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_narrow_frozen_r3_read_deep | - | - | 1710381.76 | 102622903.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_frozen_r3_read_recent | - | - | 1900559.43 | 114033566.00 | 7.7 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_online_r1_build | 8 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_narrow_online_r1_read_deep | - | - | 1748323.55 | 104899413.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_online_r1_read_recent | - | - | 1885437.77 | 113126266.00 | 7.2 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_online_r2_build | 8 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_narrow_online_r2_read_deep | - | - | 1757468.60 | 105448115.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_online_r2_read_recent | - | - | 1910521.95 | 114631316.00 | 7.7 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_online_r3_build | 8 | narrow | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_narrow_online_r3_read_deep | - | - | 1731452.43 | 103887145.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_narrow_online_r3_read_recent | - | - | 1897803.21 | 113868191.00 | 7.7 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_frozen_r1_build | 8 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_wide_frozen_r1_read_deep | - | - | 1688173.73 | 101290424.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_frozen_r1_read_recent | - | - | 1899380.10 | 113962805.00 | 8.2 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_frozen_r2_build | 8 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_wide_frozen_r2_read_deep | - | - | 1691890.29 | 101513417.00 | 14.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_frozen_r2_read_recent | - | - | 1910955.78 | 114657345.00 | 8.2 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_frozen_r3_build | 8 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_wide_frozen_r3_read_deep | - | - | 1700758.70 | 102045521.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_frozen_r3_read_recent | - | - | 1932396.77 | 115943806.00 | 8.2 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_online_r1_build | 8 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_wide_online_r1_read_deep | - | - | 1747119.54 | 104827173.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_online_r1_read_recent | - | - | 1911402.33 | 114684139.00 | 8.2 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_online_r2_build | 8 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_wide_online_r2_read_deep | - | - | 1653567.90 | 99214072.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_online_r2_read_recent | - | - | 1930962.72 | 115857760.00 | 8.2 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_online_r3_build | 8 | wide | 0.00 | 0.00 | - | 0.00 | - | -1 | True |
| m4_k8_wide_online_r3_read_deep | - | - | 1734335.01 | 104060099.00 | 13.3 | 0.00 | 0.0 | 0 | True |
| m4_k8_wide_online_r3_read_recent | - | - | 1930274.94 | 115816495.00 | 7.7 | 0.00 | 0.0 | 0 | True |


## Unresolved-effect rule

An arm-to-arm difference is only called an effect when it is at least 1.5x AND larger than the between-rep spread. Otherwise the result is `unresolved` and no parameter is named as optimal.
