// Minimal JSON value + parser/serializer used for Entry.as_dict()/serialize()
// and the environment mirror. Deliberately dependency-free so the core builds
// on any target. Sufficient for laila's serialized Entry/Constitution schema.
#ifndef LAILA_JSON_HPP
#define LAILA_JSON_HPP

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace laila_c {

class Json {
public:
  // Mirrors Python/msgpack's value model: scalars + bytes + ordered map + array.
  // Bytes is distinct from String (str vs bytes parity, msgpack use_bin_type).
  enum class Type { Null, Bool, Int, Double, String, Bytes, Array, Object };

  Json() : type_(Type::Null) {}
  Json(std::nullptr_t) : type_(Type::Null) {}
  Json(bool b) : type_(Type::Bool), bool_(b) {}
  Json(int64_t i) : type_(Type::Int), int_(i) {}
  Json(int i) : type_(Type::Int), int_(i) {}
  Json(double d) : type_(Type::Double), dbl_(d) {}
  Json(const char* s) : type_(Type::String), str_(s) {}
  Json(const std::string& s) : type_(Type::String), str_(s) {}

  static Json array() { Json j; j.type_ = Type::Array; return j; }
  static Json object() { Json j; j.type_ = Type::Object; return j; }
  static Json bytes(std::vector<uint8_t> b) {
    Json j; j.type_ = Type::Bytes; j.bytes_ = std::move(b); return j;
  }

  Type type() const { return type_; }
  bool is_null() const { return type_ == Type::Null; }
  bool is_object() const { return type_ == Type::Object; }
  bool is_array() const { return type_ == Type::Array; }
  bool is_string() const { return type_ == Type::String; }
  bool is_bytes() const { return type_ == Type::Bytes; }

  bool as_bool() const { return bool_; }
  int64_t as_int() const { return type_ == Type::Double ? (int64_t)dbl_ : int_; }
  double as_double() const { return type_ == Type::Int ? (double)int_ : dbl_; }
  const std::string& as_string() const { return str_; }
  const std::vector<uint8_t>& as_bytes() const { return bytes_; }

  // Object access (insertion-ordered, like a Python dict / msgpack map).
  Json& operator[](const std::string& key);
  bool contains(const std::string& key) const;
  const Json& at(const std::string& key) const;  // returns null sentinel if absent
  const std::vector<std::pair<std::string, Json>>& items() const { return obj_; }

  // Array access.
  void push_back(const Json& v) { arr_.push_back(v); }
  const std::vector<Json>& elements() const { return arr_; }
  std::vector<Json>& elements() { return arr_; }

  std::string dump() const;                   // compact serialization
  static Json parse(const std::string& text); // throws LailaError on malformed

private:
  void dump_to(std::string& out) const;

  Type type_;
  bool bool_ = false;
  int64_t int_ = 0;
  double dbl_ = 0.0;
  std::string str_;
  std::vector<uint8_t> bytes_;
  std::vector<Json> arr_;
  std::vector<std::pair<std::string, Json>> obj_;  // insertion-ordered
};

}  // namespace laila_c

#endif  // LAILA_JSON_HPP
