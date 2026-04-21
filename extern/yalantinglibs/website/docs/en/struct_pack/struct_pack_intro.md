# struct_pack Introduction

struct_pack is a serialization library featuring zero-cost abstraction as well as usability. In struct_pack, the serialization or deserialization of one **aggregated** structure could easily be done in a single line of code, without any DSL, macro, or template to be defined. struct_pack also support use marco for custom reflection of non-aggregated struct. struct_pack supports the serialization of C++ structures through compile-time reflection and its performance is significantly better than protobuf and msgpack (see the benchmark section for details).

Below, we show the basic usage of struct_pack with a simple object as an example.

```cpp
struct person {
  int64_t id;
  std::string name;
  int age;
  double salary;
};

person person1{.id = 1, .name = "hello struct pack", .age = 20, .salary = 1024.42};
```

## Serialization

In below we demonstrate serval ways of serialize one object with struct_pack APIs.

### Basic Usage 

```cpp
// serialization in one line
std::vector<char> result = struct_pack::serialize(person1);
```

### Explicit data container

```cpp
auto result = struct_pack::serialize<std::string>(person1);
// explicitly use std::string instead of std::vector<char> to hold the result
```

### Append the result at the end of existing data container

```cpp
std::string result="The next line is struct_pack serialize result.\n";
auto result = struct_pack::serialize_to(result,person1);
// 
```

### Save the results to memory location indicated by pointer

```cpp
auto sz=struct_pack::get_needed_siarray(person1);
std::unique_ptr array=std::make_unique<char[]>(sz);
auto result = struct_pack::serialize_to(array.get(),sz,person1);
// save the result at given memory location by pointer
```

### Multi-parameter serialization

```cpp
auto result=struct_pack::serialize(person1.id, person1.name, person1.age, person1.salary);
//serialize as std::tuple<int64_t, std::string, int, double>
```

### Save the results to output stream

```cpp
std::ofstream writer("struct_pack_demo.data",
                      std::ofstream::out | std::ofstream::binary);
struct_pack::serialize_to(writer, person1);
```

## Deserialization

In below we demonstrate serval ways of deserialize one object with struct_pack APIs.

### Basic Usage

```cpp
// deserialize in one line
auto person2 = deserialize<person>(buffer);
assert(person2); // person2.has_value() == true
assert(person2.value()==person1);
```

### deserialize from pointers

```cpp
// deserialize from memory location indicated by pointers
auto person2 = deserialize<person>(buffer.data(),buffer.size());
assert(person2); //person2.has_value() == true
assert(person2.value()==person1);
```

### deserialize to an existing object

```cpp
// deserialize to an existing object
person person2;
auto ec = deserialize_to(person2, buffer);
assert(!ec); // no error
assert(person2==person1);
```

### Multi-parameter deserialization

```cpp
auto person2 = deserialize<int64_t,std::string,int,double>(buffer);
assert(person2); // person2.has_value() == true
auto &&[id,name,age,salary]=person2.value();
assert(person1.id==id);
assert(person1.name==name);
assert(person1.age==age);
assert(person1.salary==salary);
```

### deserialize to input stream

```cpp
std::ifstream ifs("struct_pack_demo.data",
                  std::ofstream::in | std::ofstream::binary);
auto person2 = struct_pack::deserialize<person>(ifs);
assert(person2 == person1);
```


### Partial deserialization

Sometimes users only need to deserialize specific fields of an object instead of all of them, and that's when the partial deserialization feature can be used. This can avoid full deserialization and improve efficiency significantly, eg.:

```cpp
// Just deserialize the 2nd field of person 
auto name = get_field<person, 1>(buffer.data(), buffer.size());
assert(name); // name.has_value() == true
assert(name.value() == "hello struct pack");
```

## support std containers, std::optional and custom containers

For example, the library supports the following complicated objects with std containers and std::optional fields:

```cpp
enum class Color { red, black, white };

struct complicated_object {
  Color color;
  int a;
  std::string b;
  std::vector<person> c;
  std::list<std::string> d;
  std::deque<int> e;
  std::map<int, person> f;
  std::multimap<int, person> g;
  std::set<std::string> h;
  std::multiset<int> i;
  std::unordered_map<int, person> j;
  std::unordered_multimap<int, int> k;
  std::array<person, 2> m;
  person n[2];
  std::pair<std::string, person> o;
  std::optional<int> p;
  std::unique_ptr<int> q;
};

struct nested_object {
  int id;
  std::string name;
  person p;
  complicated_object o;
};

nested_object nested{.id = 2, .name = "tom", .p = {20, "tom"}, .o = {}};
auto buffer = serialize(nested);
auto nested2 = deserialize(buffer.data(), buffer.size());
assert(nested2)
assert(nested2==nested1);
```

## custom support

### custom reflection

Sometimes user need support non-aggregated type, or adjust the order of each field, which can be supported by macro function `YLT_REFL(typename, fieldname1, fieldname2 ...)`.

```cpp
namespace test {
class person : std::vector<int> {
 private:
  std::string mess;

 public:
  int age;
  std::string name;
  auto operator==(const person& rhs) const {
    return age == rhs.age && name == rhs.name;
  }
  person() = default;
  person(int age, const std::string& name) : age(age), name(name) {}
};
YLT_REFL(person, name, age);
}

The first argument of `YLT_REFL(typename, fieldname1, fieldname2 ...)` is the name of the type reflected, the others are the field names.
The macro must be defined in the same namespace of the reflected type.
The macro allow struct_pack support non-aggregated type, allow user define constructor, derived from other type or add some field which don't serialize.
```

Sometimes, user want to serialize/deserialize some private fields, which can be supported by define macro function `YLT_REFL(typename, members...)` inside of structure.
```cpp
namespace example2 {
class person {
 private:
  int age;
  std::string name;

 public:
  auto operator==(const person& rhs) const {
    return age == rhs.age && name == rhs.name;
  }
  person() = default;
  person(int age, const std::string& name) : age(age), name(name) {}
  YLT_REFL(person, age, name);
};

}  // namespace example2
```

This macro must declared inside the struct, it regist struct_pack reflection function as static member function.



User can even register member function which return left-reference as structure members, which greatly expands the flexibility of struct_pack.


```cpp
namespace example3 {
class person {
 private:
  int age_;
  std::string name_;

 public:
  auto operator==(const person& rhs) const {
    return age_ == rhs.age_ && name_ == rhs.name_;
  }
  person() = default;
  person(int age, const std::string& name) : age_(age), name_(name) {}

  int& age() { return age_; };
  const int& age() const { return age_; };
  std::string& name() { return name_; };
  const std::string& name() const { return name_; };
};
YLT_REFL(person, age(), name());
}  // namespace example3
```
The member function registed must return a reference, and this function must have a const version overload & non-const overload.


### custom type

#### The type cannot be abstracted to a type already in the type system

For example, let's say we need to serialise a map type from a third-party library (absl, boost...): we just need to make sure that it conforms to the constraints of the struct_pack type system for maps:

```cpp
// We should not inherit from stl container, this case just for testing.
template <typename Key, typename Value>
struct my_map : public std::map<Key, Value> {};

my_map<int, std::string> map1;
map1.emplace(1, "tom");
map1.emplace(2, "jerry");

absl::flat_hash_map<int, std::string> map2 =
    {{1, "huey"}, {2, "dewey"}, {3, "louie"},};

auto buffer1 = serialize(map1);
auto buffer2 = serialize(map2);
```

For more detail, See [struct_pack type system](https://alibaba.github.io/yalantinglibs/en/struct_pack/struct_pack_type_system.html)

#### The type cannot be abstracted to a type already in the type system

At this time, we also support custom serialisation, the user only needs to customise the following three functions:

1. sp_get_needed_size
2. sp_serialize_to
3. sp_deserialize_to

For example, the following is an example of support for serialisation/deserialisation of a custom 2D array type.

```cpp
struct array2D {
  unsigned int x;
  unsigned int y;
  float* p;
  array2D(unsigned int x, unsigned int y) : x(x), y(y) {
    p = (float*)calloc(1ull * x * y, sizeof(float));
  }
  array2D(const array2D&) = delete;
  array2D(array2D&& o) : x(o.x), y(o.y), p(o.p) { o.p = nullptr; };
  array2D& operator=(const array2D&) = delete;
  array2D& operator=(array2D&& o) {
    x = o.x;
    y = o.y;
    p = o.p;
    o.p = nullptr;
    return *this;
  }
  float& operator()(std::size_t i, std::size_t j) { return p[i * y + j]; }
  bool operator==(const array2D& o) const {
    return x == o.x && y == o.y &&
           memcmp(p, o.p, 1ull * x * y * sizeof(float)) == 0;
  }
  array2D() : x(0), y(0), p(nullptr) {}
  ~array2D() { free(p); }
};

// You need add those functions:

// 1. sp_get_needed_size: calculate length of serialization
std::size_t sp_get_needed_size(const array2D& ar) {
  return 2 * struct_pack::get_write_size(ar.x) +
         struct_pack::get_write_size(ar.p, 1ull * ar.x * ar.y);
}
// 2. sp_serialize_to: serilize object to writer
template </*struct_pack::writer_t*/ typename Writer>
void sp_serialize_to(Writer& writer, const array2D& ar) {
  struct_pack::write(writer, ar.x);
  struct_pack::write(writer, ar.y);
  struct_pack::write(writer, ar.p, 1ull * ar.x * ar.y);
}
// 3. sp_deserialize_to: deserilize object from reader
template </*struct_pack::reader_t*/ typename Reader>
struct_pack::err_code sp_deserialize_to(Reader& reader, array2D& ar) {
  if (auto ec = struct_pack::read(reader, ar.x); ec) {
    return ec;
  }
  if (auto ec = struct_pack::read(reader, ar.y); ec) {
    return ec;
  }
  auto length = 1ull * ar.x * ar.y * sizeof(float);
  if constexpr (struct_pack::checkable_reader_t<Reader>) {
    if (!reader.check(length)) {  // some input(such as memory) allow us check
                                  // length before read data.
      return struct_pack::errc::no_buffer_space;
    }
  }
  ar.p = (float*)malloc(length);
  auto ec = struct_pack::read(reader, ar.p, 1ull * ar.x * ar.y);
  if (ec) {
    free(ar.p);
  }
  return ec;

// 4. The default name for type checking is it's literal type name. You can also
// config it by:

// constexpr std::string_view sp_set_type_name(test*) { return "myarray2D"; }

// 5. If you want to use struct_pack::get_field/struct_pack::get_field_to, you
// need also define this function to skip deserialization of your type:

// template <typename Reader>
// struct_pack::err_code sp_deserialize_to_with_skip(Reader& reader, array2D& ar);

}

void user_defined_serialization() {
  std::vector<my_name_space::array2D> ar;
  ar.emplace_back(11, 22);
  ar.emplace_back(114, 514);
  ar[0](1, 6) = 3.14;
  ar[1](87, 111) = 2.71;
  auto buffer = struct_pack::serialize(ar);
  auto result = struct_pack::deserialize<decltype(ar)>(buffer);
  assert(result.has_value());
  assert(ar == result);
}

### custom output stream

Except std::ostream/std::sstream struct_pack also support serialize to custom output stream.

The custom stream should satisfy those conditions:

```cpp
template <typename T>
concept writer_t = requires(T t) {
  t.write((const char *)nullptr, std::size_t{}); // Output a piece of data. The return value can be implicit conversion to bool. Return false in any error. 
};
```

For example:

```cpp
// A simple output stream for fwrite.
struct fwrite_stream {
  FILE* file;
  bool write(const char* data, std::size_t sz) {
    return fwrite(data, sz, 1, file) == 1;
  }
  fwrite_stream(const char* file_name) : file(fopen(file_name, "wb")) {}
  ~fwrite_stream() { fclose(file); }
};
// ...
fwrite_stream writer("struct_pack_demo.data");
struct_pack::serialize_to(writer, person);
```

### custom input stream

Except std::istream/std::sstream struct_pack also support serialize to custom input stream.

The custom stream should satisfy those conditions:

```cpp
template <typename T>
concept reader_t = requires(T t) {
  t.read((char *)nullptr, std::size_t{}); // Input a piece of data. The return value can be implicit conversion to bool. Return false in any error. 
  t.ignore(std::size_t{}); // Skip a piece of data. The return value can be implicit conversion to bool. Return false in any error. 
  t.tellg(); // Return an unsigned integer as the absolute position of stream.
};
```

In addition, if the stream support `read_view`, then we enable the support of zero-copy for string_view.

```cpp
template <typename T>
concept view_reader_t = reader_t<T> && requires(T t) {
  { t.read_view(std::size_t{}) } -> std::convertible_to<const char *>;
  // Read a view from stream. The return value is the begin pointer to view. Return nullptr in any error.
};
```

### varint support

struct_pack also supports varint code for integer.

```cpp
{
  std::vector<struct_pack::var_int32_t> vec={-1,0,1,2,3,4,5,6,7};
  auto buffer = std::serialize(vec); //zigzag+varint code
}
{
  std::vector<struct_pack::var_uint64_t> vec={1,2,3,4,5,6,7,UINT64_MAX};
  auto buffer = std::serialize(vec); //varint code
}
struct rect {
  int a,b,c,d;
  constexpr static auto struct_pack_config = struct_pack::ENCODING_WITH_VARINT| struct_pack::USE_FAST_VARINT;
  // enable fast varint encode.
};

```

### derived class support

struct_pack supports serialize/deserialize derived class to the pointer of base class. But We need additional macro to mark the relationship to generate factory function automatically.

```cpp
//    base
//   /   |
//  obj1 obj2
//  |
//  obj3
struct base {
  uint64_t ID;
  virtual uint32_t get_struct_pack_id()
      const = 0;  // user must declare this virtual function in base
                  // class
  virtual ~base(){};
};
struct obj1 : public base {
  std::string name;
  virtual uint32_t get_struct_pack_id()
      const override;  // user must declare this virtual function in derived
                       // class
};
YLT_REFL(obj1, ID, name);
struct obj2 : public base {
  std::array<float, 5> data;
  virtual uint32_t get_struct_pack_id() const override;
};
YLT_REFL(obj2, ID, data);
struct obj3 : public obj1 {
  int age;
  virtual uint32_t get_struct_pack_id() const override;
};
YLT_REFL(obj3, ID, name, age);

STRUCT_PACK_DERIVED_DECL(base, obj1, obj2, obj3);
// declare the relationship bewteen base class and derived class.
STRUCT_PACK_DERIVED_IMPL(base, obj1, obj2, obj3);
// implement of get_struct_pack_id();
```

Then user can serialize & deserialize the type `std::unique<base>`:
```cpp
  std::vector<std::unique_ptr<base>> data;
  data.emplace_back(std::make_unique<obj2>());
  data.emplace_back(std::make_unique<obj1>());
  data.emplace_back(std::make_unique<obj3>());
  auto ret = struct_pack::serialize(data);
  auto result =
      struct_pack::deserialize<std::vector<std::unique_ptr<base>>>(ret);
  assert(result.has_value());   // check deserialize ok
  assert(result->size() == 3);  // check vector size
```

user can also serialize type `base` then deserialize it to the `std::unique<base>`:
```cpp
  auto ret = struct_pack::serialize(obj3{});
  auto result =
      struct_pack::deserialize_derived_class<base, obj1, obj2, obj3>(ret);
  assert(result.has_value());   // check deserialize ok
  std::unique_ptr<base> ptr = std::move(result.value());
  assert(ptr != nullptr);
```

#### ID collision

If two derived class has same field, it will cause ID collision because struct_pack generate the id by type hash. Two identical IDs will cause struct_pack fail to properly deserialise the derived class. struct_pack will check for such an error at compile time.

In addition, there is also a $2^-32$ probability of a hash conflict for any two different types, causing struct_pack to fail to use the hash information to check for type errors. In Debug mode, struct_pack comes with the full type string by default to mitigate hash conflicts.

The user can fix this by manually tagging the type. That is, add the member `constexpr static std::size_t struct_pack_id` to the type and assign a unique initial value. If user does not want to intrusively modify the class declaration, we can also choose to add the function `constexpr std::size_t struct_pack_id(Type*)` under the same namespace.

```cpp
struct obj1 {
  std::string name;
  virtual uint32_t get_struct_pack_id() const;
};
YLT_REFL(obj1, name);
struct obj2 : public obj1 {
  virtual uint32_t get_struct_pack_id() const override;
  constexpr static std::size_t struct_pack_id = 114514;
};
YLT_REFL(obj2, name);
```

When this field/function is added, struct_pack adds this ID to the type string, thus ensuring that the two types have different hash values, thus resolving ID conflicts/hash conflicts.



## benchmark

### Test case

The object to be serialized is pre-initialized and the memory to store the serialization result is pre-allocated. For each test case, we run one million serializations/deserialization and take the average.

### Test objects

1. A simple object `person` with 4 scaler types

```cpp
struct person {
  int64_t id;
  std::string name;
  int age;
  double salary;
};
```

2. A complicated object `monster` with nested types

```cpp
enum Color : uint8_t { Red, Green, Blue };

struct Vec3 {
  float x;
  float y;
  float z;
};

struct Weapon {
  std::string name;
  int16_t damage;
};

struct Monster {
  Vec3 pos;
  int16_t mana;
  int16_t hp;
  std::string name;
  std::vector<uint8_t> inventory;
  Color color;
  std::vector<Weapon> weapons;
  Weapon equipped;
  std::vector<Vec3> path;
};
```

3. A `rect` object with for `int32_t`

```cpp
struct rect {
  int32_t x;
  int32_t y;
  int32_t width;
  int32_t height;
};
```

### Test Environment

Compiler: Alibaba Clang 13

Processor: (Intel(R) Xeon(R) Platinum 8163 CPU @ 2.50GHz)

### Test results

![struct_pack_bench_serialize](./images/struct_pack_bench_serialize.png)       
![struct_pack_bench_deserialize](./images/struct_pack_bench_deserialize.png)   
![struct_pack_bench_binary_size](./images/struct_pack_bench_binary_size.png)  
     
## Forward/backward compatibility

If current message type no longer meets all you needs - say, you'd like the object to have an extra field, the compatibility should not be broken so that the old object could be correctly parsed with the new type definition. In struct_pack, any new fields you added must be of type `struct_pack::compatible<T, version_number>`. <br />And the version number should be incremental each time you update the struct. Let's take struct `person` as an example:

```cpp
struct person {
  int age;
  std::string name;
};

struct person_v0 {
  int age;
  std::string name;
  struct_pack::compatible<bool> maybe; //default version number is 0
};

struct person_v1 {
  int age;
  std::string name;
  struct_pack::compatible<int32_t,20230101> id; // version number is 20230101
  struct_pack::compatible<bool> maybe; 
  struct_pack::compatible<std::string,20230101> password; // version number is 20230101
};

struct person_v2 {
  int age;
  std::string name;
  struct_pack::compatible<int32_t,20230101> id; 
  struct_pack::compatible<bool> maybe; 
  struct_pack::compatible<std::string,20230101> password;
  struct_pack::compatible<double,20230402> salary; //the version number should be incremental. So 20230402 > 20230101
};
```

struct_pack ensures that those version of struct person can be safely converted to each other by serialization and deserialization, thus achieving forward/backward compatibility.

Remember，if the version number is not incremental, struct_pack can't achieve the compatibility. Thus struct_pack skip the type check for compatible field, the convert is undefined behavior!

## Why is struct_pack faster?

1. Streamlined type information and efficient type-checking. MD5 computation is done at compile time so at runtime we only needs to compare 32bit hash values to check sameness.
2. struct_pack is a template library that encourages the compiler to do a better job at inlining.
4. Zero-cost abstraction, no runtime cost for features that are not used.
5. struct_pack's memory layout is much closer to the native C++ object, reducing the workload of (de)serializations.
6. Compile-time type calculation allows struct_pack to generate different codes according to different types. So we can do optimization upon types. For example, we could use `memcpy` on contiguous containers and zero-copy optimization could be used on `string_view`

## Appendix

### struct_pack type system

[struct_pack type system](https://alibaba.github.io/yalantinglibs/en/struct_pack/struct_pack_type_system.html)

### struct_pack layout

[struct_pack layout](https://alibaba.github.io/yalantinglibs/en/struct_pack/struct_pack_layout.html)


### Test code

see [benchmark.cpp](https://github.com/alibaba/yalantinglibs/tree/main/src/struct_pack/benchmark)
