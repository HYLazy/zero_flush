#!/usr/bin/env python3
"""Render the report cover with local Noto Sans SC fonts (font_overrides)."""
import json, sys
sys.path.insert(0, "/home/embed/.zcode/cli/plugins/cache/zcode-plugins-official/pdf/0.1.7/skills/pdf/scripts")
import cover_render as cr

FONTS = "/home/embed/hyl/metadata_offload/report/fonts"
OV = {r: ("NotoSansSC", f"{FONTS}/NotoSansSC-Regular.ttf", None) for r in ("sans", "serif", "kai")}
OV["latin"] = ("NotoSansSC", f"{FONTS}/NotoSansSC-Regular.ttf", None)

PALETTE = {   # palette.cascade (mode=minimal, intent=cold) mapped onto cover roles
    "primary": "#375462", "secondary": "#5b7887", "text": "#181a1b",
    "muted": "#848b8e", "bg": "#f2f3f3",
    "ac_bg": "#1d2b33", "ac_accent": "#a14c2f", "ac_gold": "#8a7446",
    "ac_text": "#ffffff", "ac_muted": "#9fb3bd", "ac_foot": "#5b7887",
}

CONTENT = {
    "kicker": "STORAGE ENGINE PERFORMANCE · ROCKSDB 11.2.0",
    "hero": "RocksDB 写性能分析",
    "summary": "默认配置下 1KB KV / 100GB 写入负载的 CPU 归因、内存构成与瓶颈解剖："
               "写路径 74% 的 CPU 中有效做功仅约 5%，其余为并发协调与限速节流下的自旋等待；"
               "后台 compaction 线程 1→6 使 fill 墙钟缩短 24%，但 16MB/s 的默认限速上限仍是主约束。",
    "meta": "实验环境：i9-10980XE · 36 线程 · 62 GB 内存 · NVMe SSD　|　报告日期：2026 年 10 月",
    "footer_left": "bench/fillcpu · bench/wpbench 实测数据",
    "footer_right": "DOC WPERF-2026-10",
    "year": "2026", "word": "PERFORMANCE",
}

if __name__ == "__main__":
    tmpl = sys.argv[1] if len(sys.argv) > 1 else "05"
    out = sys.argv[2] if len(sys.argv) > 2 else f"cover_{tmpl}.pdf"
    cr.render_cover(tmpl, CONTENT, out, palette=PALETTE, font_overrides=OV)
    print("cover:", out)
