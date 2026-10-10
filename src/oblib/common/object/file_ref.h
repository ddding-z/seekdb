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

#ifndef OCEANBASE_COMMON_OBJECT_FILE_REF_H_
#define OCEANBASE_COMMON_OBJECT_FILE_REF_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObIJsonBase;

struct FileRef
{
  FileRef() : uri_(), mime_type_(), object_version_(), size_bytes_(-1) {}

  ObString uri_;
  ObString mime_type_;
  ObString object_version_;
  int64_t size_bytes_;
};

class FileRefCodec
{
public:
  static constexpr int64_t FORMAT_VERSION = 1;
  static constexpr int64_t MAX_REF_BYTES = 16 * 1024;
  static constexpr int64_t MAX_URI_BYTES = 4096;
  static constexpr int64_t MAX_MIME_TYPE_BYTES = 255;
  static constexpr int64_t MAX_OBJECT_VERSION_BYTES = 4096;

  // Opaque v1 payload: big-endian FILE magic, version byte, three int32 byte
  // lengths, int64 size, then URI/MIME/object-version bytes. Decode rejects
  // unsupported versions, noncanonical URIs, trailing data and malformed sizes.
  // Constructors perform no filesystem access. Only absolute local paths and
  // authority-free file:/// URIs are accepted; URI query/fragment components
  // and traversal above the root are rejected. Stored URIs are normalized and
  // percent-encoded. Encoded values and JSON results belong to the allocator.
  static int from_path(ObIAllocator &allocator, const ObString &path, ObString &encoded);
  // JSON requires format_version=1 and uri. The only optional fields are
  // mime_type (concrete type/subtype), object_version, and nonnegative size_bytes.
  static int from_json(ObIAllocator &allocator, ObIJsonBase &json, ObString &encoded);
  // Decoded strings borrow the encoded value's storage.
  static int decode(const ObString &encoded, FileRef &ref);
  static int to_json(ObIAllocator &allocator, const ObString &encoded, ObString &json);
  static int get_uri(const ObString &encoded, ObString &uri);
};
}
}

#endif
