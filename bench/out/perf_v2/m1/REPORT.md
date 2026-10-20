# perf_v2 analysis

Source: `/home/embed/hyl/metadata_offload/bench/out/perf_v2`. Service throughput only (measurement cohort); invalid runs excluded. Latency percentiles are bucket **lower bounds** from the cohort per-class histograms (hist_version 2); the bucket width is given where it matters so the resolution is visible.

## M1 (51 runs, 0 invalid/excluded)

### M1 arms (pure write)

| rid | arrival | offered | achieved | ratio | submitted | completed | submitted_over_offered | client_wait_p99_us | p50_us | p99_us | p99_vs_3x_baseline | p99_crossed_3x | bucket_ns | half_ratio | steady | pending_gib_s | stalls | wa | window_s | valid |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| m1_cb_r1 | closed | 0.00 | 83236.06 | - | 4994093 | 4994093 | 0.00 | - | 28.7 | 2359.3 | 26.19 | True | 262144 | 0.09 | False | 0.37 | 17.00 | 4.48 | 60.00 | True |
| m1_cb_r2 | closed | 0.00 | 82287.93 | - | 4937075 | 4937075 | 0.00 | - | 28.7 | 1572.9 | 17.46 | True | 131072 | 0.86 | False | 0.42 | 16.00 | 3.80 | 60.00 | True |
| m1_cb_r3 | closed | 0.00 | 71228.93 | - | 4273463 | 4273463 | 0.00 | - | 30.7 | 2359.3 | 26.19 | True | 262144 | 0.92 | False | 0.30 | 10.00 | 4.00 | 60.00 | True |
| m1_noc10_r1 | open | 83236.00 | 83236.23 | 1.00 | 4994173 | 4994173 | 1.00 | 983.04 | 45.1 | 114.7 | 1.27 | False | 8192 | 1.00 | False | 0.53 | 0.00 | 1.01 | 60.00 | True |
| m1_noc10_r2 | open | 82287.00 | 82291.00 | 1.00 | 4937458 | 4937458 | 1.00 | 180.22 | 41.0 | 106.5 | 1.18 | False | 8192 | 1.00 | False | 0.53 | 0.00 | 1.13 | 60.00 | True |
| m1_noc10_r3 | open | 71228.00 | 71230.17 | 1.00 | 4273810 | 4273810 | 1.00 | 73.73 | 45.1 | 106.5 | 1.18 | False | 8192 | 1.00 | False | 0.47 | 0.00 | 0.99 | 60.00 | True |
| m1_ov_f0.25_r1 | open | 20809.00 | 20809.06 | 1.00 | 1248543 | 1248543 | 1.00 | 62914.56 | 28.7 | 114.7 | 1.27 | False | 8192 | 1.00 | True | 0.03 | 0.00 | 10.40 | 60.00 | True |
| m1_ov_f0.25_r2 | open | 20571.00 | 20571.05 | 1.00 | 1234262 | 1234262 | 1.00 | 67108.86 | 28.7 | 114.7 | 1.27 | False | 8192 | 1.01 | False | 0.06 | 0.00 | 11.47 | 60.00 | True |
| m1_ov_f0.25_r3 | open | 17807.00 | 17807.23 | 1.00 | 1068433 | 1068433 | 1.00 | 33554.43 | 28.7 | 90.1 | 1.00 | False | 8192 | 1.00 | True | 0.00 | 0.00 | 10.11 | 60.00 | True |
| m1_ov_f0.50_r1 | open | 41618.00 | 41618.12 | 1.00 | 2497087 | 2497087 | 1.00 | 2952790.02 | 36.9 | 122.9 | 1.36 | False | 8192 | 1.01 | False | 0.17 | 0.00 | 8.74 | 60.00 | True |
| m1_ov_f0.50_r2 | open | 41143.00 | 41143.85 | 1.00 | 2468630 | 2468630 | 1.00 | 1744830.46 | 36.9 | 122.9 | 1.36 | False | 8192 | 1.00 | False | 0.19 | 0.00 | 8.31 | 60.00 | True |
| m1_ov_f0.50_r3 | open | 35614.00 | 35615.08 | 1.00 | 2136903 | 2136903 | 1.00 | 92274.69 | 28.7 | 106.5 | 1.18 | False | 8192 | 1.00 | True | 0.01 | 0.00 | 10.37 | 60.00 | True |
| m1_ov_f0.75_r1 | open | 62427.00 | 62429.81 | 1.00 | 3742892 | 3742892 | 1.00 | 3758096.38 | 36.9 | 131.1 | 1.46 | False | 16384 | 1.00 | False | 0.33 | 1.00 | 6.74 | 59.95 | True |
| m1_ov_f0.75_r1_ext300 | open | 62427.00 | 53342.01 | 0.85 | 16002538 | 16002538 | 0.85 | 34359738.37 | 32.8 | 1572.9 | 17.46 | True | 131072 | 0.71 | False | 0.22 | 15.00 | 6.31 | 300.00 | True |
| m1_ov_f0.75_r1_ext300_ext300 | open | 44966.00 | 44966.05 | 1.00 | 13489814 | 13489814 | 1.00 | 3221225.47 | 32.8 | 131.1 | 1.46 | False | 16384 | 1.03 | False | 0.18 | 0.00 | 7.04 | 300.00 | True |
| m1_ov_f0.75_r2 | open | 61715.00 | 61716.99 | 1.00 | 3703019 | 3703019 | 1.00 | 3221225.47 | 36.9 | 114.7 | 1.27 | False | 8192 | 1.00 | False | 0.41 | 0.00 | 5.69 | 60.00 | True |
| m1_ov_f0.75_r2_ext300 | open | 61715.00 | 52196.15 | 0.85 | 15535049 | 15535049 | 0.85 | 47244640.26 | 36.9 | 1572.9 | 17.46 | True | 131072 | 0.73 | False | 0.22 | 13.00 | 6.07 | 297.63 | True |
| m1_ov_f0.75_r2_ext300_ext300 | open | 40459.00 | 40459.62 | 1.00 | 12137885 | 12137885 | 1.00 | 3489660.93 | 36.9 | 147.5 | 1.64 | False | 16384 | 1.00 | False | 0.15 | 0.00 | 7.95 | 300.00 | True |
| m1_ov_f0.75_r3 | open | 53421.00 | 53514.30 | 1.00 | 3210858 | 3210858 | 1.00 | 5368709.12 | 32.8 | 122.9 | 1.36 | False | 8192 | 1.00 | False | 0.32 | 0.00 | 6.44 | 60.00 | True |
| m1_ov_f0.75_r3_ext300 | open | 53421.00 | 51479.32 | 0.96 | 15443357 | 15443357 | 0.96 | 4831838.21 | 36.9 | 196.6 | 2.18 | False | 16384 | 0.93 | False | 0.21 | 5.00 | 6.32 | 299.99 | True |
| m1_ov_f0.75_r3_ext300_ext300 | open | 51468.00 | 51443.14 | 1.00 | 15432937 | 15432937 | 1.00 | 4294967.30 | 36.9 | 147.5 | 1.64 | False | 16384 | 1.00 | False | 0.21 | 4.00 | 6.40 | 300.00 | True |
| m1_ov_f0.90_r1 | open | 74912.00 | 70652.11 | 0.94 | 4238411 | 4238411 | 0.94 | 4831838.21 | 32.8 | 1179.6 | 13.09 | True | 131072 | 0.89 | False | 0.37 | 5.00 | 5.27 | 59.99 | True |
| m1_ov_f0.90_r1_ext300 | open | 74912.00 | 52104.53 | 0.70 | 15631334 | 15631334 | 0.70 | 77309411.33 | 36.9 | 2359.3 | 26.19 | True | 262144 | 0.40 | False | 0.22 | 18.00 | 5.98 | 300.00 | True |
| m1_ov_f0.90_r1_ext300_ext300 | open | 53960.00 | 49529.74 | 0.92 | 14858921 | 14858921 | 0.92 | 17179869.18 | 36.9 | 1441.8 | 16.00 | True | 131072 | 0.84 | False | 0.22 | 9.00 | 5.96 | 300.00 | True |
| m1_ov_f0.90_r2 | open | 74059.00 | 74244.55 | 1.00 | 4454672 | 4454672 | 1.00 | 2952790.02 | 32.8 | 213.0 | 2.36 | False | 16384 | 1.00 | False | 0.35 | 4.00 | 4.60 | 60.00 | True |
| m1_ov_f0.90_r2_ext300 | open | 74059.00 | 51049.75 | 0.69 | 15314924 | 15314924 | 0.69 | 77309411.33 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.39 | False | 0.22 | 24.00 | 6.04 | 300.00 | True |
| m1_ov_f0.90_r2_ext300_ext300 | open | 48551.00 | 48553.11 | 1.00 | 14565934 | 14565934 | 1.00 | 3758096.38 | 36.9 | 131.1 | 1.46 | False | 16384 | 1.00 | False | 0.21 | 0.00 | 6.40 | 300.00 | True |
| m1_ov_f0.90_r3 | open | 64106.00 | 64682.70 | 1.01 | 3751422 | 3751422 | 1.01 | 3221225.47 | 32.8 | 163.8 | 1.82 | False | 16384 | 0.98 | False | 0.48 | 2.00 | 5.83 | 58.00 | True |
| m1_ov_f0.90_r3_ext300 | open | 64106.00 | 51268.81 | 0.80 | 15380620 | 15380620 | 0.80 | 55834574.85 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.62 | False | 0.22 | 23.00 | 5.87 | 300.00 | True |
| m1_ov_f0.90_r3_ext300_ext300 | open | 61762.00 | 52017.73 | 0.84 | 15605302 | 15605302 | 0.84 | 42949672.96 | 36.9 | 2359.3 | 26.19 | True | 262144 | 0.68 | False | 0.22 | 13.00 | 5.96 | 300.00 | True |
| m1_ov_f1.00_r1 | open | 83236.00 | 82779.10 | 0.99 | 4966591 | 4966591 | 0.99 | 5368709.12 | 30.7 | 1441.8 | 16.00 | True | 131072 | 1.40 | False | 0.49 | 8.00 | 4.94 | 60.00 | True |
| m1_ov_f1.00_r1_ext300 | open | 83236.00 | 51958.03 | 0.62 | 15587409 | 15587409 | 0.62 | 111669149.70 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.40 | False | 0.21 | 24.00 | 5.59 | 300.00 | True |
| m1_ov_f1.00_r1_ext300_ext300 | open | 59955.00 | 49717.00 | 0.83 | 14909811 | 14909811 | 0.83 | 47244640.26 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.70 | False | 0.22 | 14.00 | 5.58 | 299.89 | True |
| m1_ov_f1.00_r2 | open | 82287.00 | 86531.32 | 1.05 | 5191879 | 5191879 | 1.05 | 5905580.03 | 28.7 | 1441.8 | 16.00 | True | 131072 | 1.24 | False | 0.49 | 9.00 | 4.26 | 60.00 | True |
| m1_ov_f1.00_r2_ext300 | open | 82287.00 | 49488.98 | 0.60 | 14846368 | 14846368 | 0.60 | 111669149.70 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.35 | False | 0.22 | 23.00 | 5.83 | 299.99 | True |
| m1_ov_f1.00_r2_ext300_ext300 | open | 53946.00 | 48442.42 | 0.90 | 14532714 | 14532714 | 0.90 | 21474836.48 | 36.9 | 1572.9 | 17.46 | True | 131072 | 0.80 | False | 0.22 | 12.00 | 5.70 | 300.00 | True |
| m1_ov_f1.00_r3 | open | 71228.00 | 71230.15 | 1.00 | 4215695 | 4215695 | 1.00 | 4294967.30 | 36.9 | 1441.8 | 16.00 | True | 131072 | 0.97 | False | 0.47 | 5.00 | 5.13 | 59.18 | True |
| m1_ov_f1.00_r3_ext300 | open | 71228.00 | 47522.96 | 0.67 | 14256869 | 14256869 | 0.67 | 85899345.92 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.45 | False | 0.22 | 22.00 | 5.70 | 300.00 | True |
| m1_ov_f1.00_r3_ext300_ext300 | open | 68624.00 | 50333.71 | 0.73 | 15099997 | 15099997 | 0.73 | 68719476.74 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.48 | False | 0.22 | 20.00 | 5.71 | 300.00 | True |
| m1_ov_f1.10_r1 | open | 91559.00 | 70656.74 | 0.77 | 4204740 | 4204740 | 0.77 | 12884901.89 | 30.7 | 2359.3 | 26.19 | True | 262144 | 0.72 | False | 0.36 | 10.00 | 4.91 | 59.51 | True |
| m1_ov_f1.10_r1_ext300 | open | 91559.00 | 49786.14 | 0.54 | 14935839 | 14935839 | 0.54 | 120259084.29 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.37 | False | 0.22 | 22.00 | 5.47 | 300.00 | True |
| m1_ov_f1.10_r1_ext300_ext300 | open | 65951.00 | 47496.55 | 0.72 | 14248921 | 14248921 | 0.72 | 68719476.74 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.47 | False | 0.22 | 22.00 | 5.69 | 300.00 | True |
| m1_ov_f1.10_r2 | open | 90516.00 | 91186.87 | 1.01 | 5471175 | 5471175 | 1.01 | 8589934.59 | 28.7 | 1572.9 | 17.46 | True | 131072 | 0.87 | False | 0.51 | 13.00 | 3.89 | 60.00 | True |
| m1_ov_f1.10_r2_ext300 | open | 90516.00 | 47979.91 | 0.53 | 14393941 | 14393941 | 0.53 | 128849018.88 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.46 | False | 0.22 | 27.00 | 5.50 | 300.00 | True |
| m1_ov_f1.10_r2_ext300_ext300 | open | 59341.00 | 48145.71 | 0.81 | 14443670 | 14443670 | 0.81 | 55834574.85 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.65 | False | 0.21 | 20.00 | 5.46 | 300.00 | True |
| m1_ov_f1.10_r3 | open | 78351.00 | 77101.26 | 0.98 | 4625250 | 4625250 | 0.98 | 4294967.30 | 30.7 | 245.8 | 2.73 | False | 16384 | 0.94 | False | 0.45 | 8.00 | 4.89 | 59.99 | True |
| m1_ov_f1.10_r3_ext300 | open | 78351.00 | 47215.55 | 0.60 | 14164609 | 14164609 | 0.60 | 111669149.70 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.32 | False | 0.22 | 23.00 | 5.56 | 300.00 | True |
| m1_ov_f1.10_r3_ext300_ext300 | open | 75486.00 | 49302.87 | 0.65 | 14790860 | 14790860 | 0.65 | 94489280.51 | 32.8 | 2359.3 | 26.19 | True | 262144 | 0.35 | False | 0.22 | 25.00 | 5.20 | 300.00 | True |

Steady-state check (second vs first half of the measure window): 3 converged, 45 still moving. `half_ratio` is second-half/first-half throughput; a value at or below ~1.0 means the rate was still climbing or decaying while it was being measured.

#### Extended-window arms (the steady-state follow-up)

Arms the short window flagged as still moving, re-run on a fresh replica with a longer measurement window. These are excluded from the knee aggregation above.

### extended-window arms

| rid | offered | achieved | ratio | submitted_over_offered | client_wait_p99_us | p50_us | p99_us | p99_vs_3x_baseline | half_ratio | p99_ratio | steady | steady_reason | window_s | valid |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| m1_ov_f0.75_r1_ext300 | 62427.00 | 53342.01 | 0.85 | 0.85 | 34359738.37 | 32.8 | 1572.9 | 17.46 | 0.71 | 18.00 | False | throughput second/first half = 0.71; P99 second/first half = 18.00; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f0.75_r1_ext300_ext300 | 44966.00 | 44966.05 | 1.00 | 1.00 | 3221225.47 | 32.8 | 131.1 | 1.46 | 1.03 | 1.07 | False | pending still rising at 0.18 GiB/s | 300.00 | True |
| m1_ov_f0.75_r2_ext300 | 61715.00 | 52196.15 | 0.85 | 0.85 | 47244640.26 | 36.9 | 1572.9 | 17.46 | 0.73 | 19.20 | False | throughput second/first half = 0.73; P99 second/first half = 19.20; pending still rising at 0.22 GiB/s | 297.63 | True |
| m1_ov_f0.75_r2_ext300_ext300 | 40459.00 | 40459.62 | 1.00 | 1.00 | 3489660.93 | 36.9 | 147.5 | 1.64 | 1.00 | 1.29 | False | pending still rising at 0.15 GiB/s | 300.00 | True |
| m1_ov_f0.75_r3_ext300 | 53421.00 | 51479.32 | 0.96 | 0.96 | 4831838.21 | 36.9 | 196.6 | 2.18 | 0.93 | 1.38 | False | pending still rising at 0.21 GiB/s | 299.99 | True |
| m1_ov_f0.75_r3_ext300_ext300 | 51468.00 | 51443.14 | 1.00 | 1.00 | 4294967.30 | 36.9 | 147.5 | 1.64 | 1.00 | 1.07 | False | pending still rising at 0.21 GiB/s | 300.00 | True |
| m1_ov_f0.90_r1_ext300 | 74912.00 | 52104.53 | 0.70 | 0.70 | 77309411.33 | 36.9 | 2359.3 | 26.19 | 0.40 | 21.33 | False | throughput second/first half = 0.40; P99 second/first half = 21.33; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f0.90_r1_ext300_ext300 | 53960.00 | 49529.74 | 0.92 | 0.92 | 17179869.18 | 36.9 | 1441.8 | 16.00 | 0.84 | 1.38 | False | throughput second/first half = 0.84; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f0.90_r2_ext300 | 74059.00 | 51049.75 | 0.69 | 0.69 | 77309411.33 | 32.8 | 2359.3 | 26.19 | 0.39 | 16.00 | False | throughput second/first half = 0.39; P99 second/first half = 16.00; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f0.90_r2_ext300_ext300 | 48551.00 | 48553.11 | 1.00 | 1.00 | 3758096.38 | 36.9 | 131.1 | 1.46 | 1.00 | 1.07 | False | pending still rising at 0.21 GiB/s | 300.00 | True |
| m1_ov_f0.90_r3_ext300 | 64106.00 | 51268.81 | 0.80 | 0.80 | 55834574.85 | 32.8 | 2359.3 | 26.19 | 0.62 | 18.82 | False | throughput second/first half = 0.62; P99 second/first half = 18.82; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f0.90_r3_ext300_ext300 | 61762.00 | 52017.73 | 0.84 | 0.84 | 42949672.96 | 36.9 | 2359.3 | 26.19 | 0.68 | 19.86 | False | throughput second/first half = 0.68; P99 second/first half = 19.86; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f1.00_r1_ext300 | 83236.00 | 51958.03 | 0.62 | 0.62 | 111669149.70 | 32.8 | 2359.3 | 26.19 | 0.40 | 1.18 | False | throughput second/first half = 0.40; pending still rising at 0.21 GiB/s | 300.00 | True |
| m1_ov_f1.00_r1_ext300_ext300 | 59955.00 | 49717.00 | 0.83 | 0.83 | 47244640.26 | 32.8 | 2359.3 | 26.19 | 0.70 | 19.20 | False | throughput second/first half = 0.70; P99 second/first half = 19.20; pending still rising at 0.22 GiB/s | 299.89 | True |
| m1_ov_f1.00_r2_ext300 | 82287.00 | 49488.98 | 0.60 | 0.60 | 111669149.70 | 32.8 | 2359.3 | 26.19 | 0.35 | 1.82 | False | throughput second/first half = 0.35; P99 second/first half = 1.82; pending still rising at 0.22 GiB/s | 299.99 | True |
| m1_ov_f1.00_r2_ext300_ext300 | 53946.00 | 48442.42 | 0.90 | 0.90 | 21474836.48 | 36.9 | 1572.9 | 17.46 | 0.80 | 1.60 | False | throughput second/first half = 0.80; P99 second/first half = 1.60; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f1.00_r3_ext300 | 71228.00 | 47522.96 | 0.67 | 0.67 | 85899345.92 | 32.8 | 2359.3 | 26.19 | 0.45 | 2.35 | False | throughput second/first half = 0.45; P99 second/first half = 2.35; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f1.00_r3_ext300_ext300 | 68624.00 | 50333.71 | 0.73 | 0.73 | 68719476.74 | 32.8 | 2359.3 | 26.19 | 0.48 | 20.00 | False | throughput second/first half = 0.48; P99 second/first half = 20.00; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f1.10_r1_ext300 | 91559.00 | 49786.14 | 0.54 | 0.54 | 120259084.29 | 32.8 | 2359.3 | 26.19 | 0.37 | 10.00 | False | throughput second/first half = 0.37; P99 second/first half = 10.00; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f1.10_r1_ext300_ext300 | 65951.00 | 47496.55 | 0.72 | 0.72 | 68719476.74 | 32.8 | 2359.3 | 26.19 | 0.47 | 16.00 | False | throughput second/first half = 0.47; P99 second/first half = 16.00; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f1.10_r2_ext300 | 90516.00 | 47979.91 | 0.53 | 0.53 | 128849018.88 | 32.8 | 2359.3 | 26.19 | 0.46 | 1.11 | False | throughput second/first half = 0.46; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f1.10_r2_ext300_ext300 | 59341.00 | 48145.71 | 0.81 | 0.81 | 55834574.85 | 32.8 | 2359.3 | 26.19 | 0.65 | 16.00 | False | throughput second/first half = 0.65; P99 second/first half = 16.00; pending still rising at 0.21 GiB/s | 300.00 | True |
| m1_ov_f1.10_r3_ext300 | 78351.00 | 47215.55 | 0.60 | 0.60 | 111669149.70 | 32.8 | 2359.3 | 26.19 | 0.32 | 1.67 | False | throughput second/first half = 0.32; P99 second/first half = 1.67; pending still rising at 0.22 GiB/s | 300.00 | True |
| m1_ov_f1.10_r3_ext300_ext300 | 75486.00 | 49302.87 | 0.65 | 0.65 | 94489280.51 | 32.8 | 2359.3 | 26.19 | 0.35 | 2.00 | False | throughput second/first half = 0.35; P99 second/first half = 2.00; pending still rising at 0.22 GiB/s | 300.00 | True |

| offered fraction | median P99 (60 s) | median P99 (long) | ratio |
|---|---:|---:|---:|
| 0.75 | 122.9 µs | 172.1 µs | 1.40x |
| 0.90 | 213.0 µs | 2359.3 µs | 11.08x |
| 1.00 | 1441.8 µs | 2359.3 µs | 1.64x |
| 1.10 | 1572.9 µs | 2359.3 µs | 1.50x |

**Knee rule** (aggregated across reps): criterion `p99_gt_3x_baseline` -> median P99 1441.8 us exceeds 3x the low-load baseline 114.7 us (12.6x); per-rep P99 [1441.8, 1441.8, 1441.8]

Knee offered rate ≈ **82287 ops/s** (achieved 1.000x offered, median).

**Caveat — read this as a band, not a point.** 24 arms were re-run at a longer window and **none of them converged** (pending still rising, and at the higher rates the second half of the window delivers well under the first half). The knee location is also rep-dependent because each rep normalises against its own closed-loop ceiling, which drifted between reps. A downstream matrix (M3) should treat this number as the middle of a 40,000–50,000 ops/s band rather than a sharp threshold.

| offered ops/s | reps | median P99 µs | per-rep P99 µs | P99 vs baseline | median achieved/offered | median pending GiB/s |
|---|---:|---:|---|---:|---:|---:|
| 20571 | 3 | 114.7 | 114.7, 114.7, 90.1 | 1.00x | 1.000 | 0.028 |
| 41143 | 3 | 122.9 | 122.9, 122.9, 106.5 | 1.07x | 1.000 | 0.171 |
| 61715 | 3 | 122.9 | 131.1, 114.7, 122.9 | 1.07x | 1.000 | 0.331 |
| 74059 | 3 | 213.0 | 1179.6, 213.0, 163.8 | 1.86x | 1.003 | 0.375 |
| 82287 | 3 | 1441.8 | 1441.8, 1441.8, 1441.8 | 12.57x | 1.000 | 0.487 |
| 90516 | 3 | 1572.9 | 2359.3, 1572.9, 245.8 | 13.71x | 0.984 | 0.453 |

Lowest offered fraction (0.25) vs highest (1.10): achieved 19729 vs 58764 ops/s -> **resolved** under the §8 rule (>=1.5x and above rep spread).

45 arm(s) flagged for extension to a longer window: m1_cb_r1, m1_cb_r2, m1_cb_r3, m1_noc10_r1, m1_noc10_r2, m1_noc10_r3, m1_ov_f0.25_r2, m1_ov_f0.50_r1, m1_ov_f0.50_r2, m1_ov_f0.75_r1, m1_ov_f0.75_r1_ext300, m1_ov_f0.75_r1_ext300_ext300, m1_ov_f0.75_r2, m1_ov_f0.75_r2_ext300, m1_ov_f0.75_r2_ext300_ext300, m1_ov_f0.75_r3, m1_ov_f0.75_r3_ext300, m1_ov_f0.75_r3_ext300_ext300, m1_ov_f0.90_r1, m1_ov_f0.90_r1_ext300, m1_ov_f0.90_r1_ext300_ext300, m1_ov_f0.90_r2, m1_ov_f0.90_r2_ext300, m1_ov_f0.90_r2_ext300_ext300, m1_ov_f0.90_r3, m1_ov_f0.90_r3_ext300, m1_ov_f0.90_r3_ext300_ext300, m1_ov_f1.00_r1, m1_ov_f1.00_r1_ext300, m1_ov_f1.00_r1_ext300_ext300, m1_ov_f1.00_r2, m1_ov_f1.00_r2_ext300, m1_ov_f1.00_r2_ext300_ext300, m1_ov_f1.00_r3, m1_ov_f1.00_r3_ext300, m1_ov_f1.00_r3_ext300_ext300, m1_ov_f1.10_r1, m1_ov_f1.10_r1_ext300, m1_ov_f1.10_r1_ext300_ext300, m1_ov_f1.10_r2, m1_ov_f1.10_r2_ext300, m1_ov_f1.10_r2_ext300_ext300, m1_ov_f1.10_r3, m1_ov_f1.10_r3_ext300, m1_ov_f1.10_r3_ext300_ext300


## Unresolved-effect rule

An arm-to-arm difference is only called an effect when it is at least 1.5x AND larger than the between-rep spread. Otherwise the result is `unresolved` and no parameter is named as optimal.
