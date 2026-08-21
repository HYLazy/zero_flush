#!/bin/bash
# build_all.sh - ZeroFlush 前期实验统一编译入口
#
# 目前编译目标：
#   common 自测程序 self_test_common（验证 zipfian/timing/latency/json_out）
# 后续实验 harness（exp0~exp3）就绪后在 EXPERIMENT_TARGETS 中追加。
#
# 用法:
#   ./build_all.sh          # 编译
#   ./build_all.sh test     # 编译并运行自测（含 python 侧验证）

set -euo pipefail

CODE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${CODE_DIR}/bin"
CXX="${CXX:-g++}"
CXXFLAGS="-std=c++17 -O2 -Wall -Wextra -pthread -I${CODE_DIR}"

mkdir -p "${BIN_DIR}"

build_target() {
  local src="$1" out="$2"
  echo "[build] ${src} -> ${out}"
  ${CXX} ${CXXFLAGS} "${src}" -o "${out}"
}

# ---- 编译目标 ----
build_target "${CODE_DIR}/common/cpp/self_test.cc" "${BIN_DIR}/self_test_common"
# 占位：后续实验 harness 编译目标在此追加
# build_target "${CODE_DIR}/exp0_flush_cost/harness.cc" "${BIN_DIR}/exp0_flush_cost"

echo "[build] done."

if [[ "${1:-}" != "test" ]]; then
  exit 0
fi

# ---- 自测 ----
echo ""
echo "[test] running self_test_common ..."
"${BIN_DIR}/self_test_common"

echo ""
echo "[test] verifying json_out output with python json.loads ..."
python3 - <<'PYEOF'
import json
with open("/tmp/pre_exp_self_test.json") as f:
    obj = json.load(f)
assert obj["experiment"] == "exp0_flush_cost"
assert obj["seed"] == 42
assert obj["dry_run"] is False
assert obj["notes"] is None
assert obj["runs"][0]["value_size"] == 128
assert obj["runs"][0]["tags"] == ["fill", "wal"]
assert "hardware" in obj
print("  [PASS] /tmp/pre_exp_self_test.json parsed by json.loads")
PYEOF

echo ""
echo "[test] running exp_common.py self test ..."
python3 "${CODE_DIR}/common/py/exp_common.py"

echo ""
echo "[test] ALL SELF TESTS PASSED"
