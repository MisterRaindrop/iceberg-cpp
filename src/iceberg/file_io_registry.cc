/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "iceberg/file_io_registry.h"

#include <mutex>

namespace iceberg {

namespace {

struct RegistryState {
  std::mutex mutex;
  std::unordered_map<std::string, FileIORegistry::Factory> factories;
};

RegistryState& GetRegistryState() {
  static RegistryState state;
  return state;
}

}  // namespace

void FileIORegistry::Register(const std::string& name, Factory factory) {
  auto& state = GetRegistryState();
  std::lock_guard lock(state.mutex);
  state.factories[name] = std::move(factory);
}

Result<std::shared_ptr<FileIO>> FileIORegistry::Load(
    const std::string& name, const std::string& warehouse,
    const std::unordered_map<std::string, std::string>& properties) {
  auto& state = GetRegistryState();
  std::lock_guard lock(state.mutex);
  auto it = state.factories.find(name);
  if (it == state.factories.end()) {
    return NotFound("FileIO implementation not found: {}", name);
  }
  return it->second(warehouse, properties);
}

}  // namespace iceberg
