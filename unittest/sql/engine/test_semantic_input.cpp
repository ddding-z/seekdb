/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include "common/object/file_ref.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_input.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_semantic.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{
void require(bool condition, const char *message)
{
  if (!condition) {
    throw std::runtime_error(message);
  }
}

ObIJsonBase *parse(ObArenaAllocator &allocator, const std::string &text)
{
  ObIJsonBase *document = nullptr;
  ObString value(text.length(), text.data());
  ObString owned;
  require(OB_SUCCESS == ob_write_string(allocator, value, owned), "own native JSON fixture");
  require(OB_SUCCESS == ObJsonBaseFactory::get_json_base(&allocator, owned,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, document), "parse native JSON fixture");
  return document;
}

ObJsonObject &object(ObIJsonBase *value)
{
  require(nullptr != value && value->json_type() == ObJsonNodeType::J_OBJECT, "expected JSON object");
  return *static_cast<ObJsonObject *>(value);
}

ObJsonArray &array(ObIJsonBase *value)
{
  require(nullptr != value && value->json_type() == ObJsonNodeType::J_ARRAY, "expected JSON array");
  return *static_cast<ObJsonArray *>(value);
}

std::string text(ObIJsonBase *value)
{
  require(nullptr != value && value->json_type() == ObJsonNodeType::J_STRING, "expected JSON string");
  return std::string(value->get_data(), value->get_data_length());
}

ObJsonArray &content(ObJsonObject *body)
{
  ObJsonArray &messages = array(object(body).get_value("messages"));
  require(messages.element_count() == 1, "exactly one user message");
  ObJsonObject &message = object(messages.get_value(0));
  require(text(message.get_value("role")) == "user", "user role");
  return array(message.get_value("content"));
}

void test_ordered_native_file()
{
  ObArenaAllocator allocator;
  ObIJsonBase *document = parse(allocator, R"({
    "$seekdb_ai_input":1,
    "parts":[
      {"type":"text","value":"before"},
      {"type":"file","value":{"format_version":1,"uri":"file:///controlled/zebra.png"}},
      {"type":"text","value":"after"}
    ]
  })");
  bool found = false;
  require(OB_SUCCESS == SemanticInput::is_input_object(*document, found) && found, "recipe marker");
  SemanticInput input;
  require(OB_SUCCESS == input.append_json(allocator, *document), "parse ordered FILE recipe");
  require(input.has_files_ && input.parts_.count() == 3, "retain native FILE part");
  require(input.parts_.at(0).kind_ == SemanticInputPart::TEXT &&
          input.parts_.at(0).value_ == "before", "leading text order");
  require(input.parts_.at(1).kind_ == SemanticInputPart::FILE, "middle native FILE identity");
  require(input.parts_.at(2).kind_ == SemanticInputPart::TEXT &&
          input.parts_.at(2).value_ == "after", "trailing text order");
  ObString uri;
  require(OB_SUCCESS == FileRefCodec::get_uri(input.parts_.at(1).value_, uri) &&
          uri == "file:///controlled/zebra.png", "recipe keeps canonical reference, not media bytes");
}

void test_ordinary_json()
{
  ObArenaAllocator allocator;
  for (const char *fixture : {"{}", R"({"uri":"file:///controlled/zebra.png"})", "[]", "null"}) {
    ObIJsonBase *document = parse(allocator, fixture);
    bool found = true;
    require(OB_SUCCESS == SemanticInput::is_input_object(*document, found) && !found,
            "ordinary JSON is not a media recipe");
  }
}

void test_malformed_envelopes()
{
  const char *fixtures[] = {
    R"({"$seekdb_ai_input":2,"parts":[{"type":"text","value":"x"}]})",
    R"({"$seekdb_ai_input":1,"parts":[]})",
    R"({"$seekdb_ai_input":1,"parts":null})",
    R"({"$seekdb_ai_input":1,"parts":[{"type":"text","value":null}]})",
    R"({"$seekdb_ai_input":1,"parts":[{"type":"unknown","value":"x"}]})",
    R"({"$seekdb_ai_input":1,"parts":[{"type":"file","value":"/controlled/zebra.png"}]})",
    R"({"$seekdb_ai_input":1,"parts":[{"type":"text","value":"x","extra":1}]})",
    R"({"$seekdb_ai_input":1,"parts":[{"type":"text","value":"x"}],"extra":1})",
  };
  for (const char *fixture : fixtures) {
    ObArenaAllocator allocator;
    SemanticInput input;
    ObIJsonBase *document = parse(allocator, fixture);
    require(OB_INVALID_ARGUMENT == input.append_json(allocator, *document), "reject malformed recipe");
  }
}

void test_part_boundary()
{
  for (const int count : {64, 65}) {
    std::string fixture = R"({"$seekdb_ai_input":1,"parts":[)";
    for (int index = 0; index < count; ++index) {
      fixture += index == 0 ? "" : ",";
      fixture += R"({"type":"text","value":"x"})";
    }
    fixture += "]}";
    ObArenaAllocator allocator;
    SemanticInput input;
    ObIJsonBase *document = parse(allocator, fixture);
    require((count == 64 ? OB_SUCCESS : OB_INVALID_ARGUMENT) ==
        input.append_json(allocator, *document), "exact 64-part recipe boundary");
  }
}

void test_input_ownership()
{
  ObArenaAllocator allocator;
  SemanticInput input;
  std::string source = "original";
  ObString value(source.length(), source.data());
  require(OB_SUCCESS == input.append(allocator, SemanticInputPart::TEXT, value), "copy input");
  source.assign(source.length(), 'x');
  require(input.parts_.at(0).value_ == "original", "input survives source mutation");
}

void test_input_byte_boundary()
{
  ObArenaAllocator allocator;
  SemanticInput input;
  std::string source(ObAIFuncClient::MAX_REQUEST_BYTES, 'x');
  ObString value(source.length(), source.data());
  require(OB_SUCCESS == input.append(allocator, SemanticInputPart::TEXT, value), "exact input byte limit");
  require(OB_SIZE_OVERFLOW == input.append(allocator, SemanticInputPart::TEXT, ObString("y")),
          "one byte past input limit");
  require(input.owned_bytes_ == ObAIFuncClient::MAX_REQUEST_BYTES && input.parts_.count() == 1,
          "failed append must not change input");
}

void test_text_rendering()
{
  ObArenaAllocator allocator;
  for (const std::vector<const char *> &parts : {
      std::vector<const char *>{"ab", "c"}, std::vector<const char *>{"a", "bc"},
      std::vector<const char *>{"", "abc", ""}, std::vector<const char *>{"abc"}}) {
    SemanticInput input;
    for (const char *part : parts) {
      require(OB_SUCCESS == input.append(allocator, SemanticInputPart::TEXT, ObString(part)),
              "append text rendering fixture");
    }
    ObString rendered;
    require(OB_SUCCESS == input.render_text(allocator, rendered), "render native text input");
    require(rendered == "abc", "text rendering must preserve legacy concatenation without separators");
  }
}

void test_text_rendering_rejects_media()
{
  ObArenaAllocator allocator;
  SemanticInput empty;
  ObString rendered;
  require(OB_INVALID_ARGUMENT == empty.render_text(allocator, rendered),
          "empty native input has no renderable text");
  SemanticInput media;
  require(OB_SUCCESS == media.append(allocator, SemanticInputPart::FILE,
      ObString("file:///controlled/zebra.png")), "append media rendering fixture");
  require(OB_INVALID_ARGUMENT == media.render_text(allocator, rendered),
          "text renderer must not stringify native FILE inputs");
}

void test_shared_boolean_parser()
{
  for (const ObItemType type : {T_FUN_SYS_AI_FILTER, T_FUN_SYS_AI_JOIN}) {
    for (const bool expected : {false, true}) {
      ObArenaAllocator allocator;
      bool value = !expected;
      require(OB_SUCCESS == SemanticExprUtils::parse_boolean_result(
          type, allocator, ObString(expected ? "{\"value\":true}" : "{\"value\":false}"), value),
          "shared native/cached Boolean parser accepts exact Boolean schema");
      require(value == expected, "shared Boolean parser preserves false and true");
    }
    for (const char *fixture : {"", "true", "null", "{\"value\":1}", "{\"value\":\"true\"}",
        "{\"value\":true,\"extra\":0}", "{\"value\":true,\"value\":false}", "{"}) {
      ObArenaAllocator allocator;
      bool value = true;
      require(OB_INVALID_DATA == SemanticExprUtils::parse_boolean_result(
          type, allocator, ObString(fixture), value),
          "shared native/cached parser rejects malformed or non-Boolean results");
    }
  }
}

void test_image_body()
{
  ObArenaAllocator allocator;
  ObJsonArray &parts = array(parse(allocator, R"([
    {"type":"text","data":"question"},
    {"type":"image","mime_type":"image/png","data":"AAEC"}
  ])"));
  ObString model("vision");
  ObOpenAIUtils::ObOpenAIComplete provider;
  ObJsonObject *body = nullptr;
  require(OB_SUCCESS == provider.get_media_body(allocator, model, parts, nullptr, body), "image body");
  ObJsonArray &blocks = content(body);
  require(blocks.element_count() == 2, "two ordered image blocks");
  require(text(object(blocks.get_value(0)).get_value("text")) == "question", "instruction first");
  ObJsonObject &image = object(blocks.get_value(1));
  require(text(image.get_value("type")) == "image_url", "OpenAI image wire type");
  require(text(object(image.get_value("image_url")).get_value("url")) ==
      "data:image/png;base64,AAEC", "exact image data URI");
}

void test_audio_body()
{
  ObArenaAllocator allocator;
  ObJsonArray &parts = array(parse(allocator, R"([
    {"type":"text","data":"question"},
    {"type":"audio","format":"wav","data":"AAEC"}
  ])"));
  ObString model("audio");
  ObOpenAIUtils::ObOpenAIComplete provider;
  ObJsonObject *body = nullptr;
  require(OB_SUCCESS == provider.get_media_body(allocator, model, parts, nullptr, body), "audio body");
  ObJsonArray &blocks = content(body);
  ObJsonObject &audio = object(blocks.get_value(1));
  require(text(audio.get_value("type")) == "input_audio", "OpenAI native audio wire type");
  ObJsonObject &value = object(audio.get_value("input_audio"));
  require(text(value.get_value("data")) == "AAEC" &&
          text(value.get_value("format")) == "wav", "exact audio bytes and format");
  require(nullptr == audio.get_value("image_url"), "audio must not become an image");
}

void test_join_roles()
{
  ObArenaAllocator allocator;
  ObJsonArray &parts = array(parse(allocator, R"([
    {"type":"text","data":"rule"},
    {"type":"text","data":"LEFT INPUT"},
    {"type":"image","mime_type":"image/png","data":"AAAA"},
    {"type":"text","data":"RIGHT INPUT"},
    {"type":"image","mime_type":"image/jpeg","data":"BBBB"}
  ])"));
  ObString model("matcher");
  ObOpenAIUtils::ObOpenAIComplete provider;
  ObJsonObject *body = nullptr;
  require(OB_SUCCESS == provider.get_media_body(allocator, model, parts, nullptr, body), "join blocks");
  ObJsonArray &blocks = content(body);
  require(blocks.element_count() == 5, "retain pair roles and media order");
  require(text(object(blocks.get_value(1)).get_value("text")) == "LEFT INPUT" &&
          text(object(blocks.get_value(3)).get_value("text")) == "RIGHT INPUT", "pair role labels");
  require(text(object(object(blocks.get_value(2)).get_value("image_url")).get_value("url")) ==
      "data:image/png;base64,AAAA", "left image first");
  require(text(object(object(blocks.get_value(4)).get_value("image_url")).get_value("url")) ==
      "data:image/jpeg;base64,BBBB", "right image second");
}

void test_message_override_rejected()
{
  for (const char *fixture : {R"({"model":"override"})", R"({"messages":[]})"}) {
    ObArenaAllocator allocator;
    ObJsonArray &parts = array(parse(allocator, R"([{"type":"text","data":"question"}])"));
    ObJsonObject &config = object(parse(allocator, fixture));
    ObString model("vision");
    ObOpenAIUtils::ObOpenAIComplete provider;
    ObJsonObject *body = nullptr;
    require(OB_INVALID_ARGUMENT == provider.get_media_body(allocator, model, parts, &config, body),
            "FILE options must not replace the input/model");
    require(nullptr == body, "invalid request must not return a usable body");
  }
}

void test_boolean_schema()
{
  ObArenaAllocator allocator;
  ObJsonArray &parts = array(parse(allocator,
      R"([{"type":"image","mime_type":"image/png","data":"AAEC"}])"));
  ObJsonObject &config = object(parse(allocator, R"({
    "response_format":{"type":"json_schema","json_schema":{
      "name":"ai_filter","strict":true,
      "schema":{"type":"object","properties":{"value":{"type":"boolean"}},
                "required":["value"],"additionalProperties":false}
    }}
  })"));
  ObString model("vision");
  ObOpenAIUtils::ObOpenAIComplete provider;
  ObJsonObject *body = nullptr;
  require(OB_SUCCESS == provider.get_media_body(allocator, model, parts, &config, body),
          "multimodal Boolean schema request");
  require(text(object(body->get_value("response_format")).get_value("type")) == "json_schema",
          "retain native Boolean response format");
  ObJsonObject &valid = object(parse(allocator,
      R"({"choices":[{"message":{"content":"{\"value\":true}"},"finish_reason":"stop"}]})"));
  require(OB_SUCCESS == provider.validate_response(allocator, &valid), "validate native Boolean");
  for (const char *fixture : {
      R"({"choices":[{"message":{"content":"{\"value\":\"true\"}"},"finish_reason":"stop"}]})",
      R"({"choices":[{"message":{"content":"{\"value\":1}"},"finish_reason":"stop"}]})",
      R"({"choices":[{"message":{"content":"{\"value\":true,\"extra\":0}"},"finish_reason":"stop"}]})"}) {
    ObJsonObject &invalid = object(parse(allocator, fixture));
    require(OB_SUCCESS != provider.validate_response(allocator, &invalid),
            "multimodal output must retain strict Boolean validation");
  }
}

void test_invalid_blocks()
{
  for (const char *fixture : {
      R"([{"type":"text","data":null}])",
      R"([{"type":"image","data":"AAEC","mime_type":null}])",
      R"([{"type":"audio","data":"AAEC","format":null}])",
      R"([{"type":"file","data":"AAEC"}])", "[]"}) {
    ObArenaAllocator allocator;
    ObJsonArray &parts = array(parse(allocator, fixture));
    ObString model("vision");
    ObOpenAIUtils::ObOpenAIComplete provider;
    ObJsonObject *body = nullptr;
    require(OB_SUCCESS != provider.get_media_body(allocator, model, parts, nullptr, body),
            "reject invalid provider-neutral blocks");
    require(nullptr == body, "invalid media body must remain unavailable");
  }
}

struct TestCase
{
  const char *name_;
  void (*run_)();
};

const TestCase TESTS[] = {
  {"ordered_native_file", test_ordered_native_file},
  {"ordinary_json", test_ordinary_json},
  {"malformed_envelopes", test_malformed_envelopes},
  {"part_boundary", test_part_boundary},
  {"input_ownership", test_input_ownership},
  {"input_byte_boundary", test_input_byte_boundary},
  {"text_rendering", test_text_rendering},
  {"text_rendering_rejects_media", test_text_rendering_rejects_media},
  {"shared_boolean_parser", test_shared_boolean_parser},
  {"image_body", test_image_body},
  {"audio_body", test_audio_body},
  {"join_roles", test_join_roles},
  {"message_override_rejected", test_message_override_rejected},
  {"boolean_schema", test_boolean_schema},
  {"invalid_blocks", test_invalid_blocks},
};

int run_tests()
{
  try {
    for (const TestCase &test : TESTS) {
      test.run_();
      std::cout << "PASS " << test.name_ << std::endl;
    }
    std::cout << "semantic input native tests passed: " << sizeof(TESTS) / sizeof(TESTS[0]) << std::endl;
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "FAIL semantic input native tests: " << error.what() << std::endl;
    return 1;
  }
}
}

#ifdef FILE_INPUT_WRAP_MAIN
extern "C" int __wrap_main(int argc, char **argv)
#else
int main(int argc, char **argv)
#endif
{
  UNUSED(argc);
  UNUSED(argv);
  return run_tests();
}
