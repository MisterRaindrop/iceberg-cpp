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

#include "iceberg/catalog/hadoop/hadoop_log.h"

#include <iostream>
#include <mutex>
#include <utility>

namespace iceberg::hadoop {

namespace {

std::mutex& HandlerMutex() {
  static std::mutex m;
  return m;
}

WarningHandler& HandlerSlot() {
  static WarningHandler handler;
  return handler;
}

void DefaultHandler(std::string_view message) {
  std::cerr << "[iceberg::hadoop] WARNING: " << message << '\n';
}

}  // namespace

void SetWarningHandler(WarningHandler handler) {
  std::lock_guard lk(HandlerMutex());
  HandlerSlot() = std::move(handler);
}

void LogWarning(std::string_view message) {
  WarningHandler handler;
  {
    std::lock_guard lk(HandlerMutex());
    handler = HandlerSlot();
  }
  if (handler) {
    handler(message);
  } else {
    DefaultHandler(message);
  }
}

}  // namespace iceberg::hadoop
