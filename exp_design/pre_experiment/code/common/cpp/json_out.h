// json_out.h - header-only 最小 JSON 序列化器（无第三方依赖）
//
// 支持 object / array / number / string / bool / null，带缩进输出。
// 使用示例：
//   pre_exp::JsonWriter w;
//   w.BeginObject();
//   w.Key("name").String("exp0");
//   w.Key("runs").BeginArray().Number(1).Number(2).EndArray();
//   w.EndObject();
//   std::string s = w.Dump();
//
// C++17。

#ifndef PRE_EXP_COMMON_JSON_OUT_H_
#define PRE_EXP_COMMON_JSON_OUT_H_

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace pre_exp {

class JsonWriter {
 public:
  explicit JsonWriter(int indent_width = 2) : indent_width_(indent_width) {}

  // ---- 结构 ----
  JsonWriter& BeginObject() {
    BeforeValue();
    out_ += '{';
    stack_.push_back(Frame{kObject, false});
    return *this;
  }

  JsonWriter& EndObject() {
    const bool had = !stack_.empty() && stack_.back().has_elem;
    CloseFrame('}', had);
    return *this;
  }

  JsonWriter& BeginArray() {
    BeforeValue();
    out_ += '[';
    stack_.push_back(Frame{kArray, false});
    return *this;
  }

  JsonWriter& EndArray() {
    const bool had = !stack_.empty() && stack_.back().has_elem;
    CloseFrame(']', had);
    return *this;
  }

  // object 的 key（之后必须紧跟一个值）
  JsonWriter& Key(const std::string& k) {
    BeforeValue();
    WriteString(k);
    out_ += ':';
    if (indent_width_ > 0) out_ += ' ';
    pending_key_ = true;
    return *this;
  }

  // ---- 标量值 ----
  JsonWriter& Number(double v) {
    BeforeValue();
    char buf[64];
    if (v == static_cast<double>(static_cast<int64_t>(v)) &&
        v >= -9.007199254740992e15 && v <= 9.007199254740992e15) {
      std::snprintf(buf, sizeof(buf), "%lld",
                    static_cast<long long>(static_cast<int64_t>(v)));
    } else {
      std::snprintf(buf, sizeof(buf), "%.17g", v);
    }
    out_ += buf;
    AfterValue();
    return *this;
  }

  JsonWriter& Number(int64_t v) {
    BeforeValue();
    out_ += std::to_string(static_cast<long long>(v));
    AfterValue();
    return *this;
  }

  JsonWriter& Number(uint64_t v) {
    BeforeValue();
    out_ += std::to_string(static_cast<unsigned long long>(v));
    AfterValue();
    return *this;
  }

  JsonWriter& String(const std::string& s) {
    BeforeValue();
    WriteString(s);
    AfterValue();
    return *this;
  }

  JsonWriter& Bool(bool b) {
    BeforeValue();
    out_ += b ? "true" : "false";
    AfterValue();
    return *this;
  }

  JsonWriter& Null() {
    BeforeValue();
    out_ += "null";
    AfterValue();
    return *this;
  }

  // 直接嵌入已序列化好的 JSON 片段
  JsonWriter& Raw(const std::string& json_fragment) {
    BeforeValue();
    out_ += json_fragment;
    AfterValue();
    return *this;
  }

  // ---- 输出 ----
  std::string Dump() const { return out_; }

  // 静态便捷：转义单个字符串
  static std::string Escape(const std::string& s) {
    std::string r;
    EscapeInto(s, r);
    return r;
  }

 private:
  enum FrameKind { kObject, kArray };
  struct Frame {
    FrameKind kind;
    bool has_elem;
  };

  void BeforeValue() {
    if (!stack_.empty() && indent_width_ > 0 && !pending_key_) {
      // 值（或嵌套结构开始）前换行缩进；Key() 后的值同行
      out_ += '\n';
      out_.append(static_cast<size_t>(indent_width_) * stack_.size(), ' ');
    }
    if (pending_key_) {  // Key() 之后直接写值，不需要分隔符
      return;
    }
    if (stack_.empty()) {
      if (!out_.empty()) out_ += '\n';  // 多个顶层值（非常规用法）
      return;
    }
    Frame& f = stack_.back();
    if (f.has_elem) {
      out_ += ',';
      if (indent_width_ > 0) out_ += '\n';
      out_.append(static_cast<size_t>(indent_width_) * stack_.size(), ' ');
    }
  }

  void AfterValue() {
    pending_key_ = false;
    if (!stack_.empty()) {
      stack_.back().has_elem = true;  // 本帧已有一个完整元素
    }
  }

  void CloseFrame(char closer, bool had_elem) {
    if (!stack_.empty()) stack_.pop_back();
    if (had_elem && indent_width_ > 0) {
      out_ += '\n';
      out_.append(static_cast<size_t>(indent_width_) * stack_.size(), ' ');
    }
    out_ += closer;
    AfterValue();
  }

  static void EscapeInto(const std::string& s, std::string& r) {
    r += '"';
    for (unsigned char c : s) {
      switch (c) {
        case '"': r += "\\\""; break;
        case '\\': r += "\\\\"; break;
        case '\b': r += "\\b"; break;
        case '\f': r += "\\f"; break;
        case '\n': r += "\\n"; break;
        case '\r': r += "\\r"; break;
        case '\t': r += "\\t"; break;
        default:
          if (c < 0x20) {
            char buf[8];
            std::snprintf(buf, sizeof(buf), "\\u%04x", c);
            r += buf;
          } else {
            r += static_cast<char>(c);
          }
      }
    }
    r += '"';
  }

  void WriteString(const std::string& s) { EscapeInto(s, out_); }

  std::string out_;
  std::vector<Frame> stack_;
  int indent_width_;
  bool pending_key_ = false;
};

}  // namespace pre_exp

#endif  // PRE_EXP_COMMON_JSON_OUT_H_
