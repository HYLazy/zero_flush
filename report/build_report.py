#!/usr/bin/env python3
"""Build the RocksDB write-performance analysis report (Chinese, A4, ReportLab)."""
import csv
import json
import os

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.pdfmetrics import registerFontFamily
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (BaseDocTemplate, CondPageBreak, Frame, Image, KeepTogether,
                                PageBreak, PageTemplate, Paragraph, Spacer, Table, TableStyle)
from reportlab.platypus.tableofcontents import TableOfContents

ROOT = "/home/embed/hyl/metadata_offload"
FONTS = f"{ROOT}/report/fonts"
CHARTS = f"{ROOT}/report/charts"
OUT = f"{ROOT}/report/RocksDB_1KB_100GB_写性能分析报告.pdf"
D = json.load(open(f"{ROOT}/report/data/data.json"))
R = D["runs"]

# ── palette (pdf.py palette.cascade --mode minimal) ──
PAGE_BG = colors.HexColor("#f2f3f3")
SECTION_BG = colors.HexColor("#eaeced")
CARD_BG = colors.HexColor("#e5e7e8")
STRIPE = colors.HexColor("#ebedef")
HEADER_FILL = colors.HexColor("#375462")
COVER_BLOCK = colors.HexColor("#5b7887")
BORDER = colors.HexColor("#c7ced2")
ICON = colors.HexColor("#3f6c83")
ACCENT = colors.HexColor("#a14c2f")
ACCENT2 = colors.HexColor("#5abb40")
TEXT = colors.HexColor("#181a1b")
MUTED = colors.HexColor("#848b8e")
SUCCESS = colors.HexColor("#489762")
WARNING = colors.HexColor("#8a7446")
ERROR = colors.HexColor("#a14c44")
INFO = colors.HexColor("#4b6884")

# ── fonts ──
pdfmetrics.registerFont(TTFont("Noto", f"{FONTS}/NotoSansSC-Regular.ttf"))
pdfmetrics.registerFont(TTFont("Noto-Bold", f"{FONTS}/NotoSansSC-Bold.ttf"))
pdfmetrics.registerFont(TTFont("Mono", "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"))
registerFontFamily("Noto", normal="Noto", bold="Noto-Bold", italic="Noto", boldItalic="Noto-Bold")

# ── styles ──
S = {}
S["body"] = ParagraphStyle("body", fontName="Noto", fontSize=10, leading=17, textColor=TEXT,
                           alignment=TA_LEFT, spaceAfter=6)
S["lead"] = ParagraphStyle("lead", parent=S["body"], fontSize=10.5, leading=18, textColor=TEXT)
S["h1"] = ParagraphStyle("h1", fontName="Noto-Bold", fontSize=19, leading=25, textColor=HEADER_FILL,
                         spaceBefore=2, spaceAfter=10)
S["h2"] = ParagraphStyle("h2", fontName="Noto-Bold", fontSize=13, leading=19, textColor=HEADER_FILL,
                         spaceBefore=13, spaceAfter=6)
S["h3"] = ParagraphStyle("h3", fontName="Noto-Bold", fontSize=11, leading=16, textColor=ICON,
                         spaceBefore=9, spaceAfter=4)
S["cap"] = ParagraphStyle("cap", fontName="Noto", fontSize=8.5, leading=13, textColor=MUTED,
                          alignment=TA_CENTER, spaceBefore=4, spaceAfter=10)
S["tbl"] = ParagraphStyle("tbl", fontName="Noto", fontSize=8.2, leading=11.6, textColor=TEXT)
S["tblb"] = ParagraphStyle("tblb", parent=S["tbl"], fontName="Noto-Bold")
S["tblh"] = ParagraphStyle("tblh", fontName="Noto-Bold", fontSize=8.4, leading=12, textColor=colors.white)
S["kpi_num"] = ParagraphStyle("kpi_num", fontName="Noto-Bold", fontSize=17, leading=21, textColor=HEADER_FILL,
                              alignment=TA_CENTER)
S["kpi_lab"] = ParagraphStyle("kpi_lab", fontName="Noto", fontSize=8, leading=11, textColor=MUTED,
                              alignment=TA_CENTER)
S["callout"] = ParagraphStyle("callout", fontName="Noto", fontSize=9.5, leading=15.5, textColor=TEXT, alignment=TA_LEFT)
S["callout_t"] = ParagraphStyle("callout_t", fontName="Noto-Bold", fontSize=10.5, leading=15,
                                textColor=ACCENT)
S["toc1"] = ParagraphStyle("toc1", fontName="Noto-Bold", fontSize=10.5, leading=17, textColor=TEXT)
S["toc2"] = ParagraphStyle("toc2", fontName="Noto", fontSize=9.5, leading=15, textColor=MUTED,
                           leftIndent=12)
S["bullet"] = ParagraphStyle("bullet", parent=S["body"], bulletText="•", bulletIndent=2,
                             bulletFontName="Noto", bulletFontSize=10,
                             leftIndent=14, firstLineIndent=0, spaceAfter=5)
S["mono"] = ParagraphStyle("mono", fontName="Mono", fontSize=8, leading=11.5, textColor=TEXT)

PW, PH = A4
LM = RM = 20 * mm
TM = 20 * mm
BM = 16 * mm
CW = PW - LM - RM  # content width ~ 170mm = 482pt


def fmt(x, nd=1):
    if isinstance(x, str):
        return x
    if abs(x) >= 1000:
        return f"{x:,.0f}"
    return f"{x:.{nd}f}"


def pct(a, b):
    return f"{a / b * 100:.1f}%" if b else "—"


# ── page furniture ──
def on_page(canv, doc):
    canv.saveState()
    canv.setStrokeColor(BORDER)
    canv.setLineWidth(0.6)
    y = PH - TM + 10
    canv.line(LM, y, PW - RM, y)
    canv.setFont("Noto", 7.5)
    canv.setFillColor(MUTED)
    canv.drawString(LM, y + 4, "RocksDB 写性能分析报告 · 1KB KV / 100GB · 默认配置")
    canv.drawRightString(PW - RM, y + 4, "2026-10")
    fy = BM - 6
    canv.line(LM, fy + 10, PW - RM, fy + 10)
    canv.setFont("Noto", 8)
    canv.drawCentredString(PW / 2, fy, f"第 {canv.getPageNumber() + 1} 页")
    canv.restoreState()


class Doc(BaseDocTemplate):
    def afterFlowable(self, flowable):
        if isinstance(flowable, Paragraph):
            st = flowable.style.name
            if st in ("h1", "h2"):
                self.notify("TOCEntry", (0 if st == "h1" else 1,
                                         flowable.getPlainText(), self.page + 1))
            if st == "h1":
                self.canv.bookmarkPage(f"h{self.page}_{abs(hash(flowable.getPlainText())) % 10000}")


def chart(name, caption, width_ratio=1.0, gap_before=6):
    path = f"{CHARTS}/{name}"
    from PIL import Image as PILImage
    iw, ih = PILImage.open(path).size
    w = CW * width_ratio
    return [Spacer(1, gap_before), Image(path, width=w, height=w * ih / iw),
            Paragraph(caption, S["cap"])]


def callout(title, lines, color=ACCENT):
    inner = [[Paragraph(title, S["callout_t"])]]
    for ln in lines:
        inner.append([Paragraph(ln, S["callout"])])
    t = Table(inner, colWidths=[CW - 22])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
        ("LINEBEFORE", (0, 0), (0, -1), 2.2, color),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
    ]))
    return [Spacer(1, 4), t, Spacer(1, 10)]


def kpi_row(items):
    """items: list of (number, label)"""
    cells = [[Paragraph(n, S["kpi_num"]) for n, _ in items],
             [Paragraph(l, S["kpi_lab"]) for _, l in items]]
    t = Table(cells, colWidths=[CW / len(items)] * len(items))
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), SECTION_BG),
        ("TOPPADDING", (0, 0), (-1, 0), 9), ("BOTTOMPADDING", (0, 0), (-1, 0), 1),
        ("TOPPADDING", (0, 1), (-1, 1), 0), ("BOTTOMPADDING", (0, 1), (-1, 1), 9),
        ("LINEAFTER", (0, 0), (-2, -1), 0.5, BORDER),
    ]))
    return [Spacer(1, 2), t, Spacer(1, 12)]


def make_table(header, rows, widths, align_right=None, font_size=8.2):
    align_right = align_right or []
    style = ParagraphStyle("tt", parent=S["tbl"], fontSize=font_size, leading=font_size * 1.45)
    style_b = ParagraphStyle("ttb", parent=S["tblb"], fontSize=font_size, leading=font_size * 1.45)
    style_r = ParagraphStyle("ttr", parent=style, alignment=2)
    data = [[Paragraph(h, S["tblh"]) for h in header]]
    for r in rows:
        drow = []
        for i, cell in enumerate(r):
            st = style_b if i == 0 else (style_r if i in align_right else style)
            drow.append(Paragraph(str(cell), st))
        data.append(drow)
    w = [x * CW for x in widths]
    t = Table(data, colWidths=w, repeatRows=1, hAlign="CENTER")
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_FILL),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, STRIPE]),
        ("GRID", (0, 0), (-1, -1), 0.4, BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5), ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 2.6), ("BOTTOMPADDING", (0, 0), (-1, -1), 3.0),
    ]))
    return t


def tcaption(txt):
    return Paragraph(txt, S["cap"])


def tbl(t, cap):
    """table + caption kept on one page"""
    return [KeepTogether([t, cap])]


# ═══════════════════════════════════════════════════════════════════
story = []
A = story.append
E = story.extend

# ─────────────── 目录 ───────────────
A(Paragraph("目录", S["h1"]))
_toc = TableOfContents()
_toc.levelStyles = [S["toc1"], S["toc2"]]
A(_toc)
A(PageBreak())

# ─────────────── 摘要 ───────────────
A(Paragraph("1　摘要与关键结论", S["h1"]))
A(Paragraph(
    "本报告基于同一环境的四组实测（<b>三个 100GB 跑 + 一轮参数扫描矩阵</b>），回答默认配置下 "
    "1KB KV / 100GB 写入的 CPU 去向、内存构成与吞吐瓶颈。所有数据来自自研驱动 "
    "bench/fillcpu（只设 create_if_missing，"
    "不开 Statistics）与 bench/wpbench（逐 op CPU 与 PerfContext 拆解），"
    "并用库侧 rocksdb.cfstats 与逐线程 "
    "/proc 快照双向对账。", S["lead"]))
A(Paragraph(
    "一句话结论：<b>写入本身很便宜（5.8 µs/KV，约占全程 CPU 的 5%），真正昂贵的是"
    "“被节流时如何等待”——默认 16 MB/s 的限速上限与 64 GB 的 compaction 积压软限把 3/4 的墙钟"
    "钉在涓流状态，而 16 个写线程在此期间持续自旋，把等待算成了 CPU。</b>", S["lead"]))
E(kpi_row([("22.9→30.3 MB/s", "fill 吞吐（bg2→bg8）"),
            ("−24%", "fill 墙钟（4156→3148 s）"),
            ("5.8 µs/KV", "单线程有效写功"),
            ("1.8–2.1%", "flush 占全程 CPU"),
            ("979→1405 MB", "峰值 RSS（bg2→bg8）")]))
A(Paragraph("五条可执行的结论：", S["h2"]))
for t in [
    "<b>写路径 CPU 的九成是并发协调与自旋，不是写数据。</b>有效写功（WAL + memtable + 驱动）实测 "
    "5.76 µs/KV；16 线程下写路径 CPU 升到 31.1 µs/op，75% 的机队时间在 "
    "WriteThread::AwaitState 里自旋（perf 剖面："
    "sched_yield + 内核系统调用帧 ~20%，memtable 插入仅 ~2.5%）。",
    "<b>真正的吞吐瓶颈是 compaction 积压与限速上限。</b>100GB 跑中 73.8–74.9% 的墙钟速率 ≤16.5 MB/s"
    "（中位 11 MB/s），因为 pending compaction bytes 长期压在 64 GB 软限之上；约 26% 的时间放开全速"
    "时能跑到 62–97 MB/s（峰值 623 MB/s），贡献 70–85% 的数据。",
    "<b>加 compaction 线程有用但会饱和：</b>1→6 线程把 fill 缩短 24%（4156→3148 s）、WAF 从 9.55× 降到 "
    "8.33×、积压限速事件 −45%；但 16MB/s 限速上限仍在 74% 的时间里生效，且 6 路并发把磁盘占满"
    "（compaction 线程 65% 时间处于 D 状态），代价是 flush 单作业延迟 ×2.8、内存 +39%。",
    "<b>不要把 max_subcompactions 当作提速手段：</b>"
    "实测写放大 +35%、吞吐 −25~34%（8GB 规模），线程数膨胀并大量陷入 IO 等待。",
    "<b>写线程数不是越多越好：</b>吞吐在 16 线程时 3.3× 封顶（单线程 11551 ops/核·秒 → 16 线程 7947，"
    "效率 −31%），32 线程因超出物理核数而塌陷到单线程水平以下；后台受限时应减线程——8 线程比 16 线程"
    "省 34% CPU、只慢 10%。",
]:
    A(Paragraph(t, S["bullet"]))

A(CondPageBreak((PH - TM - BM) * 0.28))

# ─────────────── 2 实验设置 ───────────────
A(Paragraph("2　实验设置与测量口径", S["h1"]))
A(Paragraph("2.1　负载与配置", S["h2"]))
A(Paragraph(
    "硬件：Intel i9-10980XE（18 核 / 36 线程）、62 GB 内存、NVMe SSD（O_DIRECT 顺序写实测 ~950 MB/s）。"
    "软件：RocksDB 11.2.0（未修改源码，链接 librocksdb.so.11.2.0），"
    "本机构建未链接压缩库，因此默认压缩解析为不压缩。负载为 96,000,000 键 × (16B 键 + 1024B 值) = "
    "99.84 GB 应用数据，16 个写线程按全键空间伪随机序写入（fill），随后停写并等待引擎静止（drain），"
    "以便把全部 flush/compaction 计完。", S["body"]))
A(Paragraph(
    "测量边界：驱动仅设置 create_if_missing 与一个只读 "
    "EventListener（用于逐作业计时），<b>不开启 Statistics</b>，因此被测的就是库默认配置；"
    "线程级 CPU 取 /proc/self/task/&lt;tid&gt;/stat 的 "
    "utime+stime 精确快照（含 R/S/D 状态），flush 逐作业 CPU 用 "
    "CLOCK_THREAD_CPUTIME_ID 在 "
    "OnFlushBegin→OnFlushCompleted 窗口内夹逼，"
    "compaction 同时记录库自报的 CompactionJobStats.cpu_micros。", S["body"]))
A(Paragraph("2.2　四个语料的对照关系", S["h2"]))
rows = [
    ["bg2·归档", "默认：1 flush + 1 compaction 线程", "load 2–3", "7 章 <b>bg2 基线</b>旧参考"],
    ["bg2·匹配", "同上（与 bg8 相邻时段重跑）", "load ≈3", "本报告主基线"],
    ["bg8", "max_background_jobs=8（2 flush + 6 compaction）", "load 5–13", "本轮重点 + perf 采样"],
    ["扫描矩阵", "wpbench 参数扫描（线程数 / WAL / 后台 / 流水线）", "load 2–9", "拆解成本用"],
]
A(make_table(["语料", "配置差异", "外部负载", "用途"], rows, [0.14, 0.46, 0.14, 0.26]))
A(tcaption("表 2-1　本报告使用的四组数据。外部负载指机器上的其它作业（共享机器），"
           "bg8 的优势是在更高负载下取得的，故为保守下界。"))
A(Paragraph("2.3　可信度对账", S["h2"]))
rows = [
    ["进程 CPU vs 线程和", "11678.08 s", "11677.85 s", "0.003%"],
    ["compaction：驱动夹逼 vs 库自报", "2564.87 s", "2550.34 s", "0.6%"],
    ["flush：驱动作业窗口 vs cfstats(High)", "163.78 s", "160.58 s", "2.0%"],
    ["flush 输出字节 vs 应用字节", "100.23 GB", "99.84 GB", "0.4%"],
]
A(make_table(["对账项（100GB 归档跑）", "口径 A", "口径 B", "差异"], rows, [0.44, 0.2, 0.2, 0.16],
             align_right=[1, 2, 3]))
A(tcaption("表 2-2　三路独立口径一致，说明逐线程 / 逐作业归因可信。"))

A(CondPageBreak((PH - TM - BM) * 0.28))

# ─────────────── 3 吞吐与时间线 ───────────────
A(Paragraph("3　写吞吐：墙钟、时间线与节流形态", S["h1"]))
E(chart("fig2_wall_throughput.png", "图 2　fill 墙钟与吞吐对比（96M×1KB）。bg8 用 6 个 compaction 线程把 fill 从 4156 s 缩到 3148 s。"))
A(Paragraph(
    "图 1 是三个跑的 10 秒窗口写速率。可以看到两条特征：<b>①</b> 大部分时间速率贴着 16 MB/s"
    "的限速上限（红色虚线）以下涓流；<b>②</b> 每隔一段时间出现一次爆发（200–620 MB/s），"
    "把积压的 memtable 一次性灌满。这种“涓流—泄洪”交替就是默认配置下 100GB 写入的稳定形态。", S["body"]))
E(chart("fig1_timeline.png", "图 1　写速率时间线：95% 以上的窗口速率低于 100 MB/s，爆发窗口在图中截断到 260 MB/s。"))
A(Paragraph("3.1　限速把时间与数据“错配”", S["h2"]))
A(Paragraph(
    "把 fill 的每个 250ms 区间按速率是否 ≤16.5 MB/s 分成两桶，结果是：<b>约 3/4 的时间被限速，"
    "但只有 15–31% 的数据是在限速区间写掉的</b>——换句话说，写够 100GB 靠的是少数全速窗口。"
    "这也解释了为什么加写线程对总吞吐几乎没有贡献：限速状态下速率由控制器决定，而非前台线程数。", S["body"]))
E(chart("fig3_throttle_split.png", "图 3　限速区间的时间占比 vs 数据占比。bg8 的慢区间更慢（自适应速率被压得更低）、快区间更快（6 线程消化积压更快）。", 0.95))

A(CondPageBreak((PH - TM - BM) * 0.28))

# ─────────────── 4 瓶颈解剖 ───────────────
A(Paragraph("4　瓶颈解剖：compaction 积压 → 限速 → 前台等待", S["h1"]))
E(callout("机制链条（可量化）", [
    "① flush 把数据送进 L0（100.2 GB / 1550–1563 个 SST）；",
    "② 每 1 GB 进入 LSM 的数据在 100GB 规模要引发 <b>8.3–9.5 GB</b> 的 compaction 读写（WAF）；",
    "③ 单线程 compaction 排空能力有限，待压缩字节累积并长期停留在 <b>64 GB 软限</b>之上；",
    "④ 超过软限即安装限速令牌：写速率上限 = delayed_write_rate（默认 16 MB/s），"
    "积压增长时按 ×0.8 自适应下调（下限 16 KB/s）；",
    "⑤ 前台按 1 ms 粒度睡眠等待，memtable 槽位耗尽时则完全停车（bg_cv_.Wait()）。",
]))
E(chart("fig6_pending.png", "图 6　待压缩字节（实线）与 L0 文件数（点线）：积压长期贴着 64 GB 线上方运行，bg8 靠 6 个线程把波动压得更低。"))
E(chart("fig9_stall.png", "图 9　stall 事件计数：bg8 把“积压限速”砍掉 45%，但 memtable 停车反增 33%（磁盘被 compaction 占满，flush 变慢）。"))
A(Paragraph(
    "需要强调术语：这里说的 stall 就是 RocksDB 的 <b>write stall</b>，由 "
    "WriteStallCondition（kDelayed / kStopped）与原因"
    "（memtable 上限 / L0 文件数 / 待压缩字节）组成。其中 kDelayed 是<b>限速而非停写</b>——"
    "100GB 跑里它占 96% 以上的 stall 时间，写仍在以 ~11 MB/s 进行；真正的停写只在 memtable 槽位"
    "耗尽时发生（646–858 次）。另有一类等待与 stall 无关：写组排队（自旋），任何并发下都存在。", S["body"]))

A(CondPageBreak((PH - TM - BM) * 0.28))

# ─────────────── 5 CPU 归因 ───────────────
A(Paragraph("5　CPU 归因：谁在消耗算力", S["h1"]))
E(chart("fig4_cpu_attribution.png", "图 4　fill + drain 全程的 CPU 归因（逐线程 /proc 口径）。写路径约 72%，compaction 约 24%，flush 仅约 2%。"))
rows = []
for k, lab in (("bg2", "bg2·匹配"), ("bg8", "bg8")):
    r = R[k]
    tot = r["cpu_fill_total"] + r["cpu_drain_total"]
    rows.append([lab, f"{tot/1000:.2f} k", f"{r['fill_wall_s']:.0f} / {r['drain_wall_s']:.0f} s",
                 f"{(r['cpu_fill']['writer']+r['cpu_drain']['writer'])/1000:.2f} k（{pct(r['cpu_fill']['writer']+r['cpu_drain']['writer'], tot)}）",
                 f"{(r['cpu_fill']['compaction_pool']+r['cpu_drain']['compaction_pool'])/1000:.2f} k（{pct(r['cpu_fill']['compaction_pool']+r['cpu_drain']['compaction_pool'], tot)}）",
                 f"{(r['cpu_fill']['flush_pool']+r['cpu_drain']['flush_pool'])/1000:.2f} k（{pct(r['cpu_fill']['flush_pool']+r['cpu_drain']['flush_pool'], tot)}）",
                 f"{r['writer_us_op']:.1f} µs"])
A(make_table(["跑", "总 CPU", "fill/drain 墙钟", "写路径", "compaction", "flush", "写路径 µs/KV"],
             rows, [0.11, 0.11, 0.19, 0.19, 0.16, 0.13, 0.11], align_right=[1, 2, 3, 4, 5, 6],
             font_size=7.8))
A(tcaption("表 5-1　CPU 归因（核心·秒）。bg8 的总 CPU 低 3.5%，但结构变化明显：写路径 −9%、compaction +9%。"))
A(Paragraph(
    "<b>flush 的账要分开算。</b>100GB 数据对应的 flush 线程 CPU 只有 209–217 核心·秒（全程 2.1%），"
    "单作业中位 104–110 ms/64 MB；但它的<b>间接影响</b>大得多：flush 是 compaction 积压的唯一来源，"
    "而积压决定限速开关（第 4 章）。此外 flush 自身还触发 memtable 上限停车（646–858 次）。"
    "评估“减少 flush 工作量”的收益时，应把这两部分分开计价。", S["body"]))

A(CondPageBreak((PH - TM - BM) * 0.28))

# ─────────────── 6 写路径的并发成本 ───────────────
A(Paragraph("6　写路径的并发成本：自旋与协调", S["h1"]))
E(chart("fig5_write_scaling.png", "图 5　写线程数扫描（wpbench quiet，无后台干扰）：吞吐 1→16 线程 3.27×，CPU/op 4.8×，每 op 延迟 5×。"))
rows = []
sw = {int(r["threads"]): r for r in D["wpbench"]["wpbench_sweep.csv"]}
lat = {1: 6.6, 2: 11.6, 4: 15.6, 8: 21.5, 16: 32.5, 32: 254.0}
for t in (1, 2, 4, 8, 16, 32):
    r = sw[t]
    rows.append([f"{t}", f"{float(r['mbps']):.0f}", f"{float(r['mbps'])/float(sw[1]['mbps']):.2f}×",
                 f"{lat[t]:.1f} µs", f"{float(r['cpu_us_per_op']):.1f} µs",
                 f"{float(r['stime_share_pct']):.0f}%",
                 f"{int(r['ops'])/float(r['cpu_s']):,.0f}"])
A(make_table(["写线程", "吞吐 MB/s", "加速比", "每 op 延迟", "CPU/op", "stime 占比", "每核效率 ops/核·s"],
             rows, [0.11, 0.15, 0.12, 0.15, 0.13, 0.15, 0.19],
             align_right=[1, 2, 3, 4, 5, 6]))
A(tcaption("表 6-1　写线程扫描（1KB Put，quiet，2M ops）。效率最优是单线程，超过 16 线程后吞吐塌陷。"))
E(chart("fig11_writepath.png", "图 11　左：单线程写路径的成本拆解（5.76 µs/KV，其中 memtable 3.3、WAL 1.1）；右：16 线程下的三种等待策略对比。"))
A(Paragraph(
    "<b>自旋从哪来：</b>WriteThread::AwaitState 的三段式等待"
    "（先 200 次 pause，再用 sched_yield 空转最多 100 µs，"
    "最后才阻塞），默认开启（enable_write_thread_adaptive_yield=true）。"
    "16 个写线程在 36 线程机器上 yield 总能立刻返回，于是持续自旋——"
    "逐线程 CPU 完全均匀、极差不到 0.4%（539.6–541.8 s）——这正是“对称自旋”的指纹。"
    "把等待从自旋改成阻塞后，线程虽然转入睡眠，"
    "却因写组合并从 ~4 op/组 塌缩到 1–2 op/组，吞吐掉到 1/5、CPU/op 反升至 59.9 µs。", S["body"]))
A(Paragraph(
    "<b>有效的结构性优化：</b>打开 enable_pipelined_write"
    "（WAL 队列与 memtable 队列分离，写者只需等前序写者的 WAL）后，16 线程写路径 CPU 从 31.1 降到 "
    "26.3 µs/op、吞吐 +33%（430→570 MB/s）；32 线程不再塌陷（119→548 MB/s）。在 compaction 受限的 "
    "8GB 场景里吞吐不变，但 CPU/op 稳定低 12%——这是唯一在两种瓶颈下都划算的写路径开关。", S["body"]))

A(CondPageBreak((PH - TM - BM) * 0.28))

# ─────────────── 7 后台线程数 ───────────────
A(Paragraph("7　后台线程数的杠杆与代价", S["h1"]))
E(chart("fig12_bgthreads.png", "图 12　后台 compaction 线程配置扫描（8.3 GB fill）：1→3/6 线程有正收益，超过 4 个线程饱和；subcompaction 掉 25–34%。"))
rows = [
    ["fill 墙钟", f"{R['bg2']['fill_wall_s']:.0f} s", f"{R['bg8']['fill_wall_s']:.0f} s", "−24%"],
    ["fill 吞吐", f"{R['bg2']['fill_mbps']:.1f} MB/s", f"{R['bg8']['fill_mbps']:.1f} MB/s", "+32%"],
    ["drain 墙钟", f"{R['bg2']['drain_wall_s']:.0f} s", f"{R['bg8']['drain_wall_s']:.0f} s", "−25%"],
    ["WAF（compaction 读入/应用数据）", f"{R['bg2']['waf']:.2f}×", f"{R['bg8']['waf']:.2f}×", "−13%"],
    ["待压缩字节限速事件", f"{R['bg2']['stall_reasons']['pending-compaction-bytes-delays']:,}",
     f"{R['bg8']['stall_reasons']['pending-compaction-bytes-delays']:,}", "−45%"],
    ["memtable 停车事件", f"{R['bg2']['stall_reasons']['memtable-limit-stops']:,}",
     f"{R['bg8']['stall_reasons']['memtable-limit-stops']:,}", "+33%"],
    ["未限速区间平均速率", f"{R['bg2']['throttle']['fast_mbps']:.1f} MB/s", f"{R['bg8']['throttle']['fast_mbps']:.1f} MB/s", "+56%"],
    ["限速时间占比", f"{R['bg2']['throttle']['wall_share']*100:.1f}%", f"{R['bg8']['throttle']['wall_share']*100:.1f}%", "≈"],
    ["compaction 线程状态", "R 59% / D 41%", "D 65% / R 24%", "转为 IO 等待"],
    ["flush 单作业 wall（中位）", f"{R['bg2']['flush_job_median_wall_s']:.3f} s", f"{R['bg8']['flush_job_median_wall_s']:.3f} s", "×2.8"],
    ["峰值 RSS", f"{R['bg2']['mem']['rss_peak']:.0f} MB", f"{R['bg8']['mem']['rss_peak']:.0f} MB", "+39%"],
]
A(make_table(["指标", "bg2（1+1 线程）", "bg8（2+6 线程）", "变化"], rows,
             [0.42, 0.2, 0.2, 0.18], align_right=[1, 2, 3]))
A(tcaption("表 7-1　bg2 vs bg8 全指标对照。收益（吞吐、WAF、积压）与代价（flush 延迟、内存）同时增大。"))
A(Paragraph(
    "三个要点：<b>①</b> 6 个 compaction 线程把积压清得更快（限速事件 −45%、WAF −13%），"
    "但默认 16 MB/s 的限速上限仍在 74% 的时间里生效，所以墙钟只改善 24% 而非数倍；"
    "<b>②</b> 磁盘成为新的共享瓶颈——compaction 线程 65% 时间在 D 状态，"
    "flush 被挤到单作业延迟 ×2.8，memtable 停车反增；<b>③</b> 内存 +39%（6 路并发 compaction 的"
    "迭代器与输出缓冲 + glibc arena 935→1379 MB），table readers 不变（644 MB，索引块）。", S["body"]))

A(CondPageBreak((PH - TM - BM) * 0.28))

# ─────────────── 8 内存 ───────────────
A(Paragraph("8　内存构成", S["h1"]))
E(chart("fig7_memory.png", "图 7　fill 末的内存构成与峰值 RSS。table readers（SST 索引块）是最大部件，约 6.4 MB/GB 数据。"))
rows = []
for k, lab in (("bg2", "bg2"), ("bg8", "bg8")):
    m, me = R[k]["mem"], R[k]["mem_fillend"]
    rows.append([lab, f"{m['rss_peak']:.0f}", f"{me['memtbl']:.0f}", f"{me['bcache']:.1f}",
                 f"{me['treaders']:.0f}", f"{me['other']:.0f}", f"{me['mall_used']:.0f}", f"{me['arena']:.0f}"])
A(make_table(["跑", "峰值 RSS", "memtable", "block cache", "table readers", "其他", "malloc 在用", "glibc arena"],
             rows, [0.1, 0.14, 0.13, 0.15, 0.16, 0.1, 0.11, 0.11],
             align_right=[1, 2, 3, 4, 5, 6, 7]))
A(tcaption("表 8-1　内存构成（MB，fill 结束时；峰值 RSS 为全程最大）。"))
A(Paragraph(
    "malloc 视角的对账（bg2）：在用堆 679 MB ≈ memtable 27 + 索引块 643 + cache 0.3，"
    "说明“其他”并非引擎数据结构，而主要是 glibc 为 memtable arena 保留的空闲堆以及代码/线程栈。"
    "block cache 默认仅 32 MB（AutoHyperClockCache），纯写负载几乎不用；"
    "真正随数据规模增长的常驻内存是 <b>table readers</b>——打开 SST 的索引块，约 6.4 MB/GB 数据"
    "（100 GB ≈ 644 MB，且与后台线程数无关）。", S["body"]))

A(CondPageBreak((PH - TM - BM) * 0.28))

# ─────────────── 9 状态与剖面 ───────────────
A(Paragraph("9　线程状态与函数级剖面", S["h1"]))
E(chart("fig8_states.png", "图 8　R/S/D 状态构成。bg8 的 compaction 线程以 IO 等待为主（65% D），写线程以睡眠为主（78–87% S）。"))
E(chart("fig10_perf.png", "图 10　perf 自时间热点（bg8）：flush/compaction 在搬 1KB 值、迭代 memtable/块与算 XXH3 校验；写线程一多半时间在自旋等待。"))
A(Paragraph(
    "把 perf 剖面与 CPU 归因对照，可以得到清晰的成本画像："
    "<b>compaction</b> 的 CPU 花在“读块 → 归并 → 构建新块 → 校验 → 写回”（内核帧 9–14% 是页缓存写回），"
    "这部分随数据量线性增长且难以压缩；<b>flush</b> 的成本集中在 memmove（1KB 值拷贝）与块构建；"
    "<b>写线程</b>的 CPU 约 20% 直接花在自旋机制上（AwaitState + "
    "sched_yield + 内核系统调用帧 + vdso 时钟），"
    "而真正插入 memtable 只占 ~2.5%。", S["body"]))
A(Paragraph(
    "这也回答了一个常见误解：“写路径 CPU 74%”并不意味着写入很贵——"
    "它衡量的是<b>16 个线程在等待彼此时的机队开销</b>。若要评估引擎的净开销，应看单线程成本"
    "（5.76 µs/KV）或同时报告 PerfContext 的 WAL/memtable 分项。", S["body"]))

A(CondPageBreak((PH - TM - BM) * 0.28))

# ─────────────── 10 结论 ───────────────
A(Paragraph("10　结论与优化建议", S["h1"]))
E(callout("给优化方向的排序（针对 1KB / 100GB / 默认配置）", [
    "<b>1. 解开后台瓶颈优先</b>：限速上限（16 MB/s）与 64 GB 积压软限决定 74% 的墙钟。"
    "降低 WAF（更少的跨层重写）或提高 compaction 有效吞吐，是唯一能显著改变吞吐的杠杆；"
    "实测 1→6 compaction 线程给 −24% 墙钟、−45% 积压事件。",
    "<b>2. 写路径结构改造</b>：enable_pipelined_write 是唯一"
    "在两种瓶颈下都划算的开关（前台受限 +33% 吞吐；后台受限 −12% CPU）。",
    "<b>3. 线程数右调</b>：效率最优是单线程；后台受限时 8 线程比 16 线程省 34% CPU、只慢 10%。",
    "<b>4. 避免反优化</b>：不要开 max_subcompactions；"
    "不要靠“减少后台线程数”提速。",
    "<b>5. 内存侧</b>：table readers 约 6.4 MB/GB 是主要常驻成本；并发 compaction 会额外推高 RSS "
    "（6 路时 +39%），显存/内存受限时应限制后台并行度。",
]))
A(Paragraph("10.1　本报告的局限", S["h2"]))
for t in [
    "共享机器：外部负载在 3–13 之间波动，bg8 跑时负载更高，故其 24% 的优势为保守下界；"
    "绝对值不宜跨负载直接比较，趋势与结构结论不受影响。",
    "flush 逐作业 CPU 在 bg8（2 个 flush 线程）下不可靠（作业窗口合计 275.5 s > 线程实测 209.5 s）："
    "原因是个别 flush 无 SST 产出、不触发完成回调，导致按 thread_id 的配对错位；"
    "驱动已改为按 (thread_id, job_id) 配对。bg8 的 flush CPU 以线程级 209.5 s / cfstats High 行 176.8 s 为准。",
    "cfstats 的 L0 行在 bg8 中混入了 L0 层 compaction（1900 作业 / 471 s），纯 flush 必须取 High 优先队列行。",
    "perf 采样窗口覆盖 fill 前中段（每 20 s × 6 轮 × 3 角色），未覆盖 drain；"
    "采样需要临时放宽 perf_event_paranoid，跑完已恢复原值。",
]:
    A(Paragraph(t, S["bullet"]))
A(Paragraph(
    "所有原始数据、脚本与中间产物位于 bench/out/fillcpu_1kb100_bg8/ 与 "
    "bench/out/fillcpu_1kb100_bg2matched/，"
    "分析脚本见 bench/fillcpu_analyze.py、"
    "bench/bg_run_extras.py、"
    "bench/fillcpu_compare_bg.py。", S["body"]))

A(PageBreak())

# ═══════════════ 附录：详细数据表 ═══════════════
A(Paragraph("附录 A　详细数据表", S["h1"]))
A(Paragraph("A.1　三个 100GB 跑的总览", S["h2"]))
rows = []
for k, lab in (("bg2_arch", "bg2·归档（load 2–3）"), ("bg2", "bg2·匹配（load ≈3）"), ("bg8", "bg8（load 5–13）")):
    r = R[k]
    rows.append([lab, f"{r['fill_wall_s']:.0f}", f"{r['fill_mbps']:.1f}", f"{r['drain_wall_s']:.0f}",
                 f"{(r['cpu_fill_total']+r['cpu_drain_total'])/1000:.2f}", f"{r['waf']:.2f}×",
                 f"{r['comp_in_gb']:.0f}", f"{r['flush_jobs']}", f"{r['comp_jobs']}",
                 f"{r['mem']['rss_peak']:.0f}"])
A(make_table(["跑", "fill s", "MB/s", "drain s", "总 CPU k", "WAF", "comp 读入 GB", "flush 作业", "comp 作业", "峰值 RSS MB"],
             rows, [0.2, 0.08, 0.07, 0.08, 0.09, 0.08, 0.11, 0.09, 0.09, 0.11],
             align_right=list(range(1, 10)), font_size=7.6))
A(Paragraph("A.2　CPU 归因明细（核心·秒）", S["h2"]))
rows = []
for k, lab in (("bg2", "bg2·匹配"), ("bg8", "bg8")):
    r, fa, da = R[k], R[k]["cpu_fill"], R[k]["cpu_drain"]
    tot = r["cpu_fill_total"] + r["cpu_drain_total"]
    rows.append([lab, f"{fa['writer']:.0f}", f"{fa['compaction_pool']:.0f}", f"{fa['flush_pool']:.0f}",
                 f"{fa['driver']:.0f}", f"{r['cpu_fill_total']:.0f}", f"{r['cpu_drain_total']:.0f}",
                 f"{r['writer_us_op']:.1f}", pct(fa['writer'] + da['writer'], tot),
                 f"{r['comp_cpu_rdb_s']:.0f}", f"{r['cfstats_High']['cpu_sec']:.1f}"])
A(make_table(["跑", "fill 写路径", "fill compaction", "fill flush", "fill 驱动", "fill 合计", "drain 合计",
              "写路径 µs/KV", "写路径占比", "comp CPU(库)", "flush CPU(库)"],
             rows, [0.1, 0.1, 0.12, 0.09, 0.09, 0.09, 0.09, 0.1, 0.09, 0.09, 0.09],
             align_right=list(range(1, 11)), font_size=7.4))
A(Paragraph("A.3　写线程扫描（wpbench，quiet，2M ops）", S["h2"]))
rows = []
for t in (1, 2, 4, 8, 16, 32):
    r = sw[t]
    rows.append([f"{t}", f"{int(r['ops']):,}", f"{float(r['wall_s']):.2f}", f"{float(r['mbps']):.1f}",
                 f"{float(r['cpu_us_per_op']):.2f}", f"{lat[t]:.1f}", f"{float(r['utime_s']):.1f}",
                 f"{float(r['stime_s']):.1f}", f"{float(r['stime_share_pct']):.0f}%",
                 f"{int(r['ops'])/float(r['cpu_s']):,.0f}"])
A(make_table(["线程", "ops", "wall s", "MB/s", "CPU µs/op", "延迟 µs", "utime s", "stime s", "stime 占比", "ops/核·s"],
             rows, [0.08, 0.12, 0.09, 0.09, 0.11, 0.09, 0.1, 0.1, 0.11, 0.11],
             align_right=list(range(1, 10)), font_size=7.6))
A(Paragraph("A.4　写路径组件与开关（wpbench）", S["h2"]))
rows = []
TAGS = [("gen_only", "仅生成键值（gen，不开库）"), ("put_quiet_wal", "单线程 Put（WAL 开）"),
        ("put_quiet_nowal", "单线程 Put（disableWAL）"), ("batch8_quiet", "单线程 WriteBatch×8（WAL 开）"),
        ("put_default_1t", "单线程默认配置（2M ops）"), ("put_quiet_t16", "16 线程 quiet（WAL 开）"),
        ("put_quiet_t16_noadapt", "16 线程 + 阻塞等待（adaptive_yield=0）"),
        ("put16_wal0", "16 线程 + disableWAL"), ("put_default_t16_noperf", "16 线程默认配置（8GB）")]
allrows = []
for f in ("wpbench.csv", "wpbench_extra.csv", "wpbench_perf.csv", "wpbench_perf_default8g.csv", "pipelined.csv"):
    path = f"{ROOT}/report/data/{f}" if f == "pipelined.csv" else f"{ROOT}/bench/out/wpbench/{f}"
    allrows += list(csv.DictReader(open(path)))
by_tag = {}
for r in allrows:
    by_tag.setdefault(r.get("tag", ""), r)
for tag, note in TAGS + [("quiet_t16_pipelined_on", "16 线程 + pipelined write（非默认）"),
                         ("quiet_t32_pipelined_off", "32 线程（默认写路径）"),
                         ("quiet_t32_pipelined_on", "32 线程 + pipelined write"),
                         ("default8g_t16_pipelined_off", "16 线程默认配置 8GB（pipelined 关）"),
                         ("default8g_t16_pipelined_on_r2", "16 线程默认配置 8GB（pipelined 开）")]:
    r = by_tag.get(tag)
    if not r:
        continue
    rows.append([note, f"{float(r['wall_s']):.2f}", f"{float(r['cpu_us_per_op']):.2f}",
                 f"{float(r['mbps']):.0f}", r["threads"], r["wal"]])
A(make_table(["配置", "wall s", "写路径 CPU µs/op", "MB/s", "线程", "WAL"],
             rows, [0.44, 0.12, 0.18, 0.1, 0.08, 0.08], align_right=[1, 2, 3, 4, 5], font_size=7.4))
A(tcaption("表 A-1　wpbench 变体明细（均为 2M ops，除非标注 8GB=8M ops）。"
           "pipelined 行的 16/32 线程与 8GB 三行为同批重复测量。"))
A(Paragraph("A.5　后台线程 / 子压缩扫描", S["h2"]))
rows = []
for r in D["bg_sweep"]:
    rows.append([r["cfg"], f"{r['mbps']}", f"{r['us_op']:.1f}" if r["us_op"] else "n/a"])
A(make_table(["配置", "吞吐 MB/s", "写路径 CPU µs/op"], rows, [0.5, 0.25, 0.25],
             align_right=[1, 2], font_size=7.6))
A(Paragraph("A.6　内存构成（MB）", S["h2"]))
rows = []
for k, lab in (("bg2", "bg2·匹配"), ("bg8", "bg8")):
    m, me = R[k]["mem"], R[k]["mem_fillend"]
    rows.append([lab, f"{m['rss_peak']:.0f}", f"{m['memtbl_peak']:.0f}", f"{m['treaders_peak']:.0f}",
                 f"{m['bcache_peak']:.1f}", f"{m['pending_peak']:.0f}", f"{me['memtbl']:.0f}",
                 f"{me['treaders']:.0f}", f"{me['other']:.0f}", f"{me['mall_used']:.0f}", f"{me['arena']:.0f}"])
A(make_table(["跑", "峰值 RSS", "memtbl 峰", "readers 峰", "cache 峰", "pending 峰 GB", "fill末 memtbl",
              "fill末 readers", "fill末 其他", "malloc 在用", "glibc arena"],
             rows, [0.1, 0.1, 0.09, 0.1, 0.09, 0.11, 0.1, 0.1, 0.09, 0.1, 0.1],
             align_right=list(range(1, 11)), font_size=7.2))
A(Paragraph("A.7　stall 事件与原因", S["h2"]))
keys = ["pending-compaction-bytes-delays", "memtable-limit-stops", "l0-file-count-limit-delays",
        "l0-file-count-limit-stops", "pending-compaction-bytes-stops", "total-delays", "total-stops"]
rows = []
for k, lab in (("bg2", "bg2·匹配"), ("bg8", "bg8")):
    sr = R[k]["stall_reasons"]
    rows.append([lab] + [f"{sr.get(x, 0):,}" for x in keys])
A(make_table(["跑"] + [x.replace("-", "<br/>") for x in keys], rows, [0.12] + [0.126] * 7,
             align_right=list(range(1, 8)), font_size=7.2))
A(Paragraph("A.8　perf 自时间热点（%）", S["h2"]))
SHORT = {"__memmove_evex_unaligned_erms": "memmove(1KB 拷贝)",
         "rocksdb::MemTableIterator::key() const": "MemTableIterator::key",
         "rocksdb::CompactionIterator::NextFromInput()": "CompactionIterator::Next",
         "XXH3_hashLong_64b_default.constprop.0": "XXH3 块校验",
         "rocksdb::CompactionMergingIterator::Next()": "MergingIterator::Next",
         "__sched_yield": "__sched_yield", "__memcmp_evex_movbe": "memcmp",
         "__tls_get_addr": "__tls_get_addr"}


def sh(sym):
    if sym in SHORT:
        return SHORT[sym]
    if sym.startswith("rocksdb::BlockBuilder::AddWithLastKeyImpl"):
        return "BlockBuilder::Add"
    if sym.startswith("rocksdb::WriteThread::AwaitState"):
        return "WriteThread::AwaitState"
    if sym.startswith("rocksdb::clock_cache"):
        return "clock_cache::TryInsert"
    if "ffffffff" in sym or sym.startswith("0x") or set(sym) <= set("0"):
        return "内核帧（写回/调度）"
    return sym[:26]


rows = []
for role, rl in (("flush", "flush"), ("comp", "compaction"), ("writer", "writer")):
    for i in range(5):
        a = D["perf"]["bg2"][role][i] if len(D["perf"]["bg2"][role]) > i else [0, "—"]
        b = D["perf"]["bg8"][role][i] if len(D["perf"]["bg8"][role]) > i else [0, "—"]
        rows.append([rl if i == 0 else "", sh(b[1]), f"{b[0]:.1f}", sh(a[1]), f"{a[0]:.1f}"])
A(make_table(["角色", "bg8 热点", "bg8 %", "bg2 热点", "bg2 %"], rows,
             [0.12, 0.34, 0.1, 0.34, 0.1], align_right=[2, 4], font_size=7.4))
A(Paragraph("A.9　生效的默认配置（库自报）", S["h2"]))
cfg = [
    ("write_buffer_size / max_write_buffer_number", "64 MB / 2"),
    ("max_background_jobs（及其推导）", "2 → flush = max(1, 2/4) = 1；compaction = 1；max_subcompactions = 1"),
    ("level0 触发（trigger / slowdown / stop）", "4 / 20 / 36"),
    ("target_file_size_base / max_bytes_for_level_base", "64 MB / 256 MB"),
    ("level_compaction_dynamic_level_bytes", "true"),
    ("compression", "kNoCompression（本机构建未链接压缩库）"),
    ("block cache（cache_index_and_filter_blocks）", "AutoHyperClockCache 32 MB（false）"),
    ("soft / hard pending compaction bytes", "64 GB / 256 GB"),
    ("delayed_write_rate", "16 MB/s（限速上限；按积压 ×0.8 / ×1.25 自适应，下限 16 KB/s）"),
    ("write_thread_max_yield_usec（adaptive yield）", "100 µs（enable_write_thread_adaptive_yield = true）"),
    ("enable_pipelined_write", "false（实测打开可显著改善多写者效率）"),
    ("max_open_files", "-1（table readers 常驻 ≈6.4 MB/GB 数据）"),
]
A(make_table(["选项", "默认值"], [[a, f"{b}"] for a, b in cfg],
             [0.42, 0.58], font_size=7.8))

# ── pagination guards (avoid orphan captions AND orphan headings) ──
bound = []
i = 0
while i < len(story):
    item = story[i]
    nxt = story[i + 1] if i + 1 < len(story) else None
    if (isinstance(item, Table) and isinstance(nxt, Paragraph)
            and getattr(nxt.style, "name", "") == "cap"):
        bound.append(KeepTogether([item, nxt]))
        i += 2
        continue
    if (isinstance(item, Paragraph) and getattr(item.style, "name", "") in ("h2", "h3")
            and isinstance(nxt, (Table, KeepTogether, Paragraph))):
        if isinstance(nxt, KeepTogether) and len(nxt._content) and isinstance(nxt._content[0], Table):
            bound.append(KeepTogether([item] + list(nxt._content)))
        else:
            bound.append(KeepTogether([item, nxt]))
        i += 2
        continue
    bound.append(item)
    i += 1
story = bound

# ── build ──
doc = Doc(OUT, pagesize=A4, leftMargin=LM, rightMargin=RM, topMargin=TM, bottomMargin=BM,
          title="RocksDB 写性能分析报告：1KB KV / 100GB",
          author="Z.ai", subject="RocksDB 默认配置下的写性能、CPU 归因与内存构成实测")
frame = Frame(LM, BM, CW, PH - TM - BM, id="main")
doc.addPageTemplates([PageTemplate(id="all", frames=[frame], onPage=on_page)])
doc.multiBuild(story)
print("PDF:", OUT)
