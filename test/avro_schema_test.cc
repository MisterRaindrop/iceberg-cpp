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

#include <string>

#include <avro/Compiler.hh>
#include <avro/NodeImpl.hh>
#include <avro/Types.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/avro/avro_reader.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/json_internal.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/name_mapping.h"
#include "iceberg/schema.h"
#include "matchers.h"

namespace iceberg::avro {

namespace {

void CheckCustomLogicalType(const ::avro::NodePtr& node, const std::string& type_name) {
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::CUSTOM);
  ASSERT_TRUE(node->logicalType().customLogicalType() != nullptr);
  EXPECT_EQ(node->logicalType().customLogicalType()->name(), type_name);
}

void CheckFieldIdAt(const ::avro::NodePtr& node, size_t index, int32_t field_id,
                    const std::string& key = "field-id") {
  ASSERT_LT(index, node->customAttributes());
  const auto& attrs = node->customAttributesAt(index);
  ASSERT_EQ(attrs.getAttribute(key), std::make_optional(std::to_string(field_id)));
}

}  // namespace

TEST(ToAvroNodeVisitorTest, BooleanType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(BooleanType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_BOOL);
}

TEST(ToAvroNodeVisitorTest, IntType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(IntType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_INT);
}

TEST(ToAvroNodeVisitorTest, LongType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(LongType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_LONG);
}

TEST(ToAvroNodeVisitorTest, FloatType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(FloatType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_FLOAT);
}

TEST(ToAvroNodeVisitorTest, DoubleType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(DoubleType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_DOUBLE);
}

TEST(ToAvroNodeVisitorTest, DecimalType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(DecimalType{10, 2}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_FIXED);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::DECIMAL);

  EXPECT_EQ(node->logicalType().precision(), 10);
  EXPECT_EQ(node->logicalType().scale(), 2);
  EXPECT_EQ(node->name().simpleName(), "decimal_10_2");
}

TEST(ToAvroNodeVisitorTest, DateType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(DateType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_INT);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::DATE);
}

TEST(ToAvroNodeVisitorTest, TimeType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(TimeType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_LONG);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::TIME_MICROS);
}

TEST(ToAvroNodeVisitorTest, TimestampType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(TimestampType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_LONG);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::TIMESTAMP_MICROS);

  ASSERT_EQ(node->customAttributes(), 1);
  EXPECT_EQ(node->customAttributesAt(0).getAttribute("adjust-to-utc"), "false");
}

TEST(ToAvroNodeVisitorTest, TimestampTzType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(TimestampTzType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_LONG);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::TIMESTAMP_MICROS);

  ASSERT_EQ(node->customAttributes(), 1);
  EXPECT_EQ(node->customAttributesAt(0).getAttribute("adjust-to-utc"), "true");
}

TEST(ToAvroNodeVisitorTest, StringType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(StringType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_STRING);
}

TEST(ToAvroNodeVisitorTest, UuidType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(UuidType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_FIXED);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::UUID);

  EXPECT_EQ(node->fixedSize(), 16);
  EXPECT_EQ(node->name().fullname(), "uuid_fixed");
}

TEST(ToAvroNodeVisitorTest, FixedType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(FixedType{20}, &node), IsOk());

  EXPECT_EQ(node->type(), ::avro::AVRO_FIXED);
  EXPECT_EQ(node->fixedSize(), 20);
  EXPECT_EQ(node->name().fullname(), "fixed_20");
}

TEST(ToAvroNodeVisitorTest, BinaryType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(BinaryType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_BYTES);
}

TEST(ToAvroNodeVisitorTest, StructType) {
  StructType struct_type{
      {SchemaField{/*field_id=*/1, "bool_field", std::make_shared<BooleanType>(),
                   /*optional=*/false},
       SchemaField{/*field_id=*/2, "int_field", std::make_shared<IntType>(),
                   /*optional=*/true}}};

  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(struct_type, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_RECORD);

  ASSERT_EQ(node->names(), 2);
  EXPECT_EQ(node->nameAt(0), "bool_field");
  EXPECT_EQ(node->nameAt(1), "int_field");

  ASSERT_EQ(node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/1, /*field_id=*/2));

  ASSERT_EQ(node->leaves(), 2);
  ASSERT_EQ(node->leafAt(0)->type(), ::avro::AVRO_BOOL);
  ASSERT_EQ(node->leafAt(1)->type(), ::avro::AVRO_UNION);
  ASSERT_EQ(node->leafAt(1)->leaves(), 2);
  EXPECT_EQ(node->leafAt(1)->leafAt(0)->type(), ::avro::AVRO_NULL);
  EXPECT_EQ(node->leafAt(1)->leafAt(1)->type(), ::avro::AVRO_INT);
}

TEST(ToAvroNodeVisitorTest, ListType) {
  ListType list_type{SchemaField{/*field_id=*/5, "element",
                                 std::make_shared<StringType>(),
                                 /*optional=*/true}};

  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(list_type, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_ARRAY);

  ASSERT_EQ(node->customAttributes(), 1);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/5,
                                         /*key=*/"element-id"));

  ASSERT_EQ(node->leaves(), 1);
  EXPECT_EQ(node->leafAt(0)->type(), ::avro::AVRO_UNION);
  ASSERT_EQ(node->leafAt(0)->leaves(), 2);
  EXPECT_EQ(node->leafAt(0)->leafAt(0)->type(), ::avro::AVRO_NULL);
  EXPECT_EQ(node->leafAt(0)->leafAt(1)->type(), ::avro::AVRO_STRING);
}

TEST(ToAvroNodeVisitorTest, MapTypeWithStringKey) {
  MapType map_type{SchemaField{/*field_id=*/10, "key", std::make_shared<StringType>(),
                               /*optional=*/false},
                   SchemaField{/*field_id=*/11, "value", std::make_shared<IntType>(),
                               /*optional=*/false}};

  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(map_type, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_MAP);

  ASSERT_GT(node->customAttributes(), 0);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/10,
                                         /*key=*/"key-id"));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/11,
                                         /*key=*/"value-id"));

  ASSERT_EQ(node->leaves(), 2);
  EXPECT_EQ(node->leafAt(0)->type(), ::avro::AVRO_STRING);
  EXPECT_EQ(node->leafAt(1)->type(), ::avro::AVRO_INT);
}

TEST(ToAvroNodeVisitorTest, MapTypeWithNonStringKey) {
  MapType map_type{SchemaField{/*field_id=*/10, "key", std::make_shared<IntType>(),
                               /*optional=*/false},
                   SchemaField{/*field_id=*/11, "value", std::make_shared<StringType>(),
                               /*optional=*/false}};

  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(map_type, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_ARRAY);
  CheckCustomLogicalType(node, "map");

  ASSERT_EQ(node->leaves(), 1);
  auto record_node = node->leafAt(0);
  ASSERT_EQ(record_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(record_node->name().fullname(), "k10_v11");

  ASSERT_EQ(record_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(record_node, /*index=*/0, /*field_id=*/10));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(record_node, /*index=*/1, /*field_id=*/11));

  ASSERT_EQ(record_node->names(), 2);
  EXPECT_EQ(record_node->nameAt(0), "key");
  EXPECT_EQ(record_node->nameAt(1), "value");

  ASSERT_EQ(record_node->leaves(), 2);
  EXPECT_EQ(record_node->leafAt(0)->type(), ::avro::AVRO_INT);
  EXPECT_EQ(record_node->leafAt(1)->type(), ::avro::AVRO_STRING);
}

TEST(ToAvroNodeVisitorTest, InvalidMapKeyType) {
  MapType map_type{SchemaField{/*field_id=*/1, "key", std::make_shared<StringType>(),
                               /*optional=*/true},
                   SchemaField{/*field_id=*/2, "value", std::make_shared<StringType>(),
                               /*optional=*/false}};

  ::avro::NodePtr node;
  auto status = ToAvroNodeVisitor{}.Visit(map_type, &node);
  EXPECT_THAT(status, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(status, HasErrorMessage("Map key `key` must be required"));
}

TEST(ToAvroNodeVisitorTest, NestedTypes) {
  auto inner_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField{/*field_id=*/2, "string_field", std::make_shared<StringType>(),
                  /*optional=*/false},
      SchemaField{/*field_id=*/3, "int_field", std::make_shared<IntType>(),
                  /*optional=*/true}});
  auto inner_list = std::make_shared<ListType>(SchemaField{/*field_id=*/5, "element",
                                                           std::make_shared<DoubleType>(),
                                                           /*optional=*/false});
  StructType root_struct{{SchemaField{/*field_id=*/1, "struct_field", inner_struct,
                                      /*optional=*/false},
                          SchemaField{/*field_id=*/4, "list_field", inner_list,
                                      /*optional=*/true}}};

  ::avro::NodePtr root_node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(root_struct, &root_node), IsOk());
  EXPECT_EQ(root_node->type(), ::avro::AVRO_RECORD);

  ASSERT_EQ(root_node->names(), 2);
  EXPECT_EQ(root_node->nameAt(0), "struct_field");
  EXPECT_EQ(root_node->nameAt(1), "list_field");

  ASSERT_EQ(root_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(root_node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(root_node, /*index=*/1, /*field_id=*/4));

  // Check struct field
  auto struct_node = root_node->leafAt(0);
  ASSERT_EQ(struct_node->type(), ::avro::AVRO_RECORD);
  ASSERT_EQ(struct_node->names(), 2);
  EXPECT_EQ(struct_node->nameAt(0), "string_field");
  EXPECT_EQ(struct_node->nameAt(1), "int_field");

  ASSERT_EQ(struct_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(struct_node, /*index=*/0, /*field_id=*/2));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(struct_node, /*index=*/1, /*field_id=*/3));

  ASSERT_EQ(struct_node->leaves(), 2);
  EXPECT_EQ(struct_node->leafAt(0)->type(), ::avro::AVRO_STRING);
  EXPECT_EQ(struct_node->leafAt(1)->type(), ::avro::AVRO_UNION);
  ASSERT_EQ(struct_node->leafAt(1)->leaves(), 2);
  EXPECT_EQ(struct_node->leafAt(1)->leafAt(0)->type(), ::avro::AVRO_NULL);
  EXPECT_EQ(struct_node->leafAt(1)->leafAt(1)->type(), ::avro::AVRO_INT);

  // Check list field
  auto list_union_node = root_node->leafAt(1);
  ASSERT_EQ(list_union_node->type(), ::avro::AVRO_UNION);
  ASSERT_EQ(list_union_node->leaves(), 2);
  EXPECT_EQ(list_union_node->leafAt(0)->type(), ::avro::AVRO_NULL);
  EXPECT_EQ(list_union_node->leafAt(1)->type(), ::avro::AVRO_ARRAY);

  auto list_node = list_union_node->leafAt(1);
  ASSERT_EQ(list_node->type(), ::avro::AVRO_ARRAY);

  ASSERT_EQ(list_node->customAttributes(), 1);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(list_node, /*index=*/0, /*field_id=*/5,
                                         /*key=*/"element-id"));

  ASSERT_EQ(list_node->leaves(), 1);
  EXPECT_EQ(list_node->leafAt(0)->type(), ::avro::AVRO_DOUBLE);
}

TEST(HasIdVisitorTest, HasNoIds) {
  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString("\"string\"")), IsOk());
  EXPECT_TRUE(visitor.HasNoIds());
  EXPECT_FALSE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, RecordWithFieldIds) {
  const std::string schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "int_field", "type": "int", "field-id": 1},
      {"name": "string_field", "type": "string", "field-id": 2}
    ]
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, RecordWithMissingFieldIds) {
  const std::string schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "int_field", "type": "int", "field-id": 1},
      {"name": "string_field", "type": "string"}
    ]
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_FALSE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, ArrayWithElementId) {
  const std::string schema_json = R"({
    "type": "array",
    "items": "int",
    "element-id": 5
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, ArrayWithoutElementId) {
  const std::string schema_json = R"({
    "type": "array",
    "items": "int"
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_FALSE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, MapWithIds) {
  const std::string schema_json = R"({
    "type": "map",
    "values": "int",
    "key-id": 10,
    "value-id": 11
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, MapWithPartialIds) {
  const std::string schema_json = R"({
    "type": "map",
    "values": "int",
    "key-id": 10
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_FALSE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, UnionType) {
  const std::string schema_json = R"([
    "null",
    {
      "type": "record",
      "name": "record_in_union",
      "fields": [
        {"name": "int_field", "type": "int", "field-id": 1}
      ]
    }
  ])";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, ComplexNestedSchema) {
  const std::string schema_json = R"({
    "type": "record",
    "name": "root",
    "fields": [
      {
        "name": "string_field",
        "type": "string",
        "field-id": 1
      },
      {
        "name": "record_field",
        "type": {
          "type": "record",
          "name": "nested",
          "fields": [
            {
              "name": "int_field",
              "type": "int",
              "field-id": 3
            }
          ]
        },
        "field-id": 2
      },
      {
        "name": "array_field",
        "type": {
          "type": "array",
          "items": "double",
          "element-id": 5
        },
        "field-id": 4
      }
    ]
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, ArrayBackedMapWithIds) {
  const std::string schema_json = R"({
    "type": "array",
    "items": {
      "type": "record",
      "name": "key_value",
      "fields": [
        {"name": "key", "type": "int", "field-id": 10},
        {"name": "value", "type": "string", "field-id": 11}
      ]
    },
    "logicalType": "map"
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, ArrayBackedMapWithPartialIds) {
  const std::string schema_json = R"({
    "type": "array",
    "items": {
      "type": "record",
      "name": "key_value",
      "fields": [
        {"name": "key", "type": "int", "field-id": 10},
        {"name": "value", "type": "string"}
      ]
    },
    "logicalType": "map"
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_FALSE(visitor.AllHaveIds());
}

TEST(AvroSchemaProjectionTest, ProjectIdenticalSchemas) {
  // Create an iceberg schema
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", std::make_shared<LongType>()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", std::make_shared<StringType>()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", std::make_shared<IntType>()),
      SchemaField::MakeRequired(/*field_id=*/4, "data", std::make_shared<DoubleType>()),
  });

  // Create equivalent avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "name", "type": ["null", "string"], "field-id": 2},
      {"name": "age", "type": ["null", "int"], "field-id": 3},
      {"name": "data", "type": "double", "field-id": 4}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 4);
  for (size_t i = 0; i < projection.fields.size(); ++i) {
    ASSERT_EQ(projection.fields[i].kind, FieldProjection::Kind::kProjected);
    ASSERT_EQ(std::get<1>(projection.fields[i].from), i);
  }
}

TEST(AvroSchemaProjectionTest, ProjectSubsetSchema) {
  // Create a subset iceberg schema
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", std::make_shared<LongType>()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", std::make_shared<IntType>()),
  });

  // Create full avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "name", "type": ["null", "string"], "field-id": 2},
      {"name": "age", "type": ["null", "int"], "field-id": 3},
      {"name": "data", "type": "double", "field-id": 4}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].from), 2);
}

TEST(AvroSchemaProjectionTest, ProjectWithPruning) {
  // Create a subset iceberg schema
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", std::make_shared<LongType>()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", std::make_shared<IntType>()),
  });

  // Create full avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "name", "type": ["null", "string"], "field-id": 2},
      {"name": "age", "type": ["null", "int"], "field-id": 3},
      {"name": "data", "type": "double", "field-id": 4}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].from), 1);
}

TEST(AvroSchemaProjectionTest, ProjectMissingOptionalField) {
  // Create iceberg schema with an extra optional field
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", std::make_shared<LongType>()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", std::make_shared<StringType>()),
      SchemaField::MakeOptional(/*field_id=*/10, "extra", std::make_shared<StringType>()),
  });

  // Create avro schema without the extra field
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "name", "type": ["null", "string"], "field-id": 2}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 3);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].from), 1);
  ASSERT_EQ(projection.fields[2].kind, FieldProjection::Kind::kNull);
}

TEST(AvroSchemaProjectionTest, ProjectMissingRequiredField) {
  // Create iceberg schema with a required field that's missing from the avro schema
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", std::make_shared<LongType>()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", std::make_shared<StringType>()),
      SchemaField::MakeRequired(/*field_id=*/10, "extra", std::make_shared<StringType>()),
  });

  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "name", "type": ["null", "string"], "field-id": 2}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Missing required field"));
}

TEST(AvroSchemaProjectionTest, ProjectMetadataColumn) {
  // Create iceberg schema with a metadata column
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", std::make_shared<LongType>()),
      MetadataColumns::kFilePath,
  });

  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kMetadata);
}

TEST(AvroSchemaProjectionTest, ProjectSchemaEvolutionIntToLong) {
  // Create iceberg schema expecting a long
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", std::make_shared<LongType>()),
  });

  // Create avro schema with an int
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "int", "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
}

TEST(AvroSchemaProjectionTest, ProjectSchemaEvolutionFloatToDouble) {
  // Create iceberg schema expecting a double
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value", std::make_shared<DoubleType>()),
  });

  // Create avro schema with a float
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "value", "type": "float", "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
}

TEST(AvroSchemaProjectionTest, ProjectSchemaEvolutionIncompatibleTypes) {
  // Create iceberg schema expecting an int
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value", std::make_shared<IntType>()),
  });

  // Create avro schema with a string
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "value", "type": "string", "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Cannot read"));
}

TEST(AvroSchemaProjectionTest, ProjectNestedStructures) {
  // Create iceberg schema with nested struct
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", std::make_shared<LongType>()),
      SchemaField::MakeOptional(
          /*field_id=*/3, "address",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeOptional(/*field_id=*/101, "street",
                                        std::make_shared<StringType>()),
              SchemaField::MakeOptional(/*field_id=*/102, "city",
                                        std::make_shared<StringType>()),
          })),
  });

  // Create equivalent avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "address", "type": ["null", {
        "type": "record",
        "name": "address_record",
        "fields": [
          {"name": "street", "type": ["null", "string"], "field-id": 101},
          {"name": "city", "type": ["null", "string"], "field-id": 102}
        ]
      }], "field-id": 3}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].from), 1);

  // Verify struct field has children correctly mapped
  ASSERT_EQ(projection.fields[1].children.size(), 2);
  ASSERT_EQ(projection.fields[1].children[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].children[0].from), 0);
  ASSERT_EQ(projection.fields[1].children[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].children[1].from), 1);
}

TEST(AvroSchemaProjectionTest, ProjectListType) {
  // Create iceberg schema with a list
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", std::make_shared<LongType>()),
      SchemaField::MakeOptional(
          /*field_id=*/2, "numbers",
          std::make_shared<ListType>(SchemaField::MakeOptional(
              /*field_id=*/101, "element", std::make_shared<IntType>()))),
  });

  // Create equivalent avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "numbers", "type": ["null", {
        "type": "array",
        "items": ["null", "int"],
        "element-id": 101
      }], "field-id": 2}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].from), 1);
}

TEST(AvroSchemaProjectionTest, ProjectMapType) {
  // Create iceberg schema with a string->int map
  Schema expected_schema({
      SchemaField::MakeOptional(
          /*field_id=*/1, "counts",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(/*field_id=*/101, "key",
                                        std::make_shared<StringType>()),
              SchemaField::MakeOptional(/*field_id=*/102, "value",
                                        std::make_shared<IntType>()))),
  });

  // Create equivalent avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "counts", "type": ["null", {
        "type": "map",
        "values": ["null", "int"],
        "key-id": 101,
        "value-id": 102
      }], "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[0].children.size(), 2);
}

TEST(AvroSchemaProjectionTest, ProjectMapTypeWithNonStringKey) {
  // Create iceberg schema with an int->string map
  Schema expected_schema({
      SchemaField::MakeOptional(
          /*field_id=*/1, "counts",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(/*field_id=*/101, "key",
                                        std::make_shared<IntType>()),
              SchemaField::MakeOptional(/*field_id=*/102, "value",
                                        std::make_shared<StringType>()))),
  });

  // Create equivalent avro schema (using array-backed map for non-string keys)
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "counts", "type": ["null", {
        "type": "array",
        "items": {
          "type": "record",
          "name": "key_value",
          "fields": [
            {"name": "key", "type": "int", "field-id": 101},
            {"name": "value", "type": ["null", "string"], "field-id": 102}
          ]
        },
        "logicalType": "map"
      }], "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[0].children.size(), 2);
}

TEST(AvroSchemaProjectionTest, ProjectListOfStruct) {
  // Create iceberg schema with list of struct
  Schema expected_schema({
      SchemaField::MakeOptional(
          /*field_id=*/1, "items",
          std::make_shared<ListType>(SchemaField::MakeOptional(
              /*field_id=*/101, "element",
              std::make_shared<StructType>(std::vector<SchemaField>{
                  SchemaField::MakeOptional(/*field_id=*/102, "x",
                                            std::make_shared<IntType>()),
                  SchemaField::MakeRequired(/*field_id=*/103, "y",
                                            std::make_shared<StringType>()),
              })))),
  });

  // Create equivalent avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "items", "type": ["null", {
        "type": "array",
        "items": ["null", {
          "type": "record",
          "name": "element_record",
          "fields": [
            {"name": "x", "type": ["null", "int"], "field-id": 102},
            {"name": "y", "type": "string", "field-id": 103}
          ]
        }],
        "element-id": 101
      }], "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);

  // Verify list element struct is properly projected
  ASSERT_EQ(projection.fields[0].children.size(), 1);
  const auto& element_proj = projection.fields[0].children[0];
  ASSERT_EQ(element_proj.children.size(), 2);
  ASSERT_EQ(element_proj.children[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(element_proj.children[0].from), 0);
  ASSERT_EQ(element_proj.children[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(element_proj.children[1].from), 1);
}

TEST(AvroSchemaProjectionTest, ProjectDecimalType) {
  // Create iceberg schema with decimal
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value",
                                std::make_shared<DecimalType>(18, 2)),
  });

  // Create avro schema with decimal
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {
        "name": "value",
        "type": {
          "type": "fixed",
          "name": "decimal_9_2",
          "size": 4,
          "logicalType": "decimal",
          "precision": 9,
          "scale": 2
        },
        "field-id": 1
      }
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
}

TEST(AvroSchemaProjectionTest, ProjectDecimalIncompatible) {
  // Create iceberg schema with decimal having different scale
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value",
                                std::make_shared<DecimalType>(18, 3)),
  });

  // Create avro schema with decimal
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {
        "name": "value",
        "type": {
          "type": "fixed",
          "name": "decimal_9_2",
          "size": 4,
          "logicalType": "decimal",
          "precision": 9,
          "scale": 2
        },
        "field-id": 1
      }
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Cannot read"));
}

}  // namespace iceberg::avro

// NameMapping tests for Avro schema context
namespace iceberg::avro {

std::unique_ptr<NameMapping> CreateTestNameMapping() {
  std::vector<MappedField> fields;
  fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});
  fields.emplace_back(MappedField{.names = {"name"}, .field_id = 2});

  // Create nested mapping for the data field
  std::vector<MappedField> nested_fields;
  nested_fields.emplace_back(MappedField{.names = {"value"}, .field_id = 3});
  nested_fields.emplace_back(MappedField{.names = {"description"}, .field_id = 4});
  auto nested_mapping = MappedFields::Make(std::move(nested_fields));

  fields.emplace_back(MappedField{
      .names = {"data"}, .field_id = 5, .nested_mapping = std::move(nested_mapping)});

  return NameMapping::Make(std::move(fields));
}

TEST(NameMappingBasicTest, Basic) {
  auto name_mapping = CreateTestNameMapping();

  // Test that the name mapping can be created and accessed
  EXPECT_TRUE(name_mapping != nullptr);
  EXPECT_EQ(name_mapping->AsMappedFields().Size(), 3);
}

TEST(NameMappingFindMethodsTest, FindMethodsWorkWithConst) {
  auto name_mapping = CreateTestNameMapping();
  const NameMapping& const_mapping = *name_mapping;

  // Test that Find methods work on const objects
  auto field_by_id = const_mapping.Find(1);
  ASSERT_TRUE(field_by_id.has_value());
  EXPECT_EQ(field_by_id->get().field_id, 1);
  EXPECT_THAT(field_by_id->get().names, testing::UnorderedElementsAre("id"));

  auto field_by_name = const_mapping.Find("name");
  ASSERT_TRUE(field_by_name.has_value());
  EXPECT_EQ(field_by_name->get().field_id, 2);
  EXPECT_THAT(field_by_name->get().names, testing::UnorderedElementsAre("name"));

  auto field_by_parts = const_mapping.Find(std::vector<std::string>{"data", "value"});
  ASSERT_TRUE(field_by_parts.has_value());
  EXPECT_EQ(field_by_parts->get().field_id, 3);
  EXPECT_THAT(field_by_parts->get().names, testing::UnorderedElementsAre("value"));
}

TEST(NameMappingFromJsonTest, FromJsonWorks) {
  std::string json_str = R"([
    {"field-id": 1, "names": ["id"]},
    {"field-id": 2, "names": ["name"]},
    {"field-id": 3, "names": ["data"], "fields": [
      {"field-id": 4, "names": ["value"]},
      {"field-id": 5, "names": ["description"]}
    ]}
  ])";

  auto result = iceberg::NameMappingFromJson(nlohmann::json::parse(json_str));
  ASSERT_TRUE(result.has_value());

  auto mapping = std::move(result.value());
  const NameMapping& const_mapping = *mapping;

  // Test that the parsed mapping works correctly
  auto field_by_id = const_mapping.Find(1);
  ASSERT_TRUE(field_by_id.has_value());
  EXPECT_EQ(field_by_id->get().field_id, 1);
  EXPECT_THAT(field_by_id->get().names, testing::UnorderedElementsAre("id"));

  auto field_by_name = const_mapping.Find("name");
  ASSERT_TRUE(field_by_name.has_value());
  EXPECT_EQ(field_by_name->get().field_id, 2);
  EXPECT_THAT(field_by_name->get().names, testing::UnorderedElementsAre("name"));

  auto nested_field = const_mapping.Find("data.value");
  ASSERT_TRUE(nested_field.has_value());
  EXPECT_EQ(nested_field->get().field_id, 4);
  EXPECT_THAT(nested_field->get().names, testing::UnorderedElementsAre("value"));
}

TEST(NameMappingNestedFieldsTest, WithNestedFields) {
  auto name_mapping = CreateTestNameMapping();
  const NameMapping& const_mapping = *name_mapping;

  // Test nested field access
  auto data_field = const_mapping.Find("data");
  ASSERT_TRUE(data_field.has_value());
  EXPECT_EQ(data_field->get().field_id, 5);
  EXPECT_THAT(data_field->get().names, testing::UnorderedElementsAre("data"));

  // Test that nested mapping exists
  ASSERT_TRUE(data_field->get().nested_mapping != nullptr);
  EXPECT_EQ(data_field->get().nested_mapping->Size(), 2);

  // Test nested field access through the nested mapping
  auto value_field = data_field->get().nested_mapping->Field(3);
  ASSERT_TRUE(value_field.has_value());
  EXPECT_EQ(value_field->get().field_id, 3);
  EXPECT_THAT(value_field->get().names, testing::UnorderedElementsAre("value"));

  auto description_field = data_field->get().nested_mapping->Field(4);
  ASSERT_TRUE(description_field.has_value());
  EXPECT_EQ(description_field->get().field_id, 4);
  EXPECT_THAT(description_field->get().names,
              testing::UnorderedElementsAre("description"));
}

TEST(NameMappingReaderOptionsTest, FromReaderOptionsWorks) {
  // Create a name mapping
  auto name_mapping = CreateTestNameMapping();
  ASSERT_TRUE(name_mapping != nullptr);
  EXPECT_EQ(name_mapping->AsMappedFields().Size(), 3);

  // Create reader options with name mapping
  ReaderOptions options;
  options.name_mapping = std::move(name_mapping);

  // Verify that the name mapping is accessible
  ASSERT_TRUE(options.name_mapping != nullptr);
  EXPECT_EQ(options.name_mapping->AsMappedFields().Size(), 3);

  // Test that the name mapping works correctly
  auto field_by_id = options.name_mapping->Find(1);
  ASSERT_TRUE(field_by_id.has_value());
  EXPECT_EQ(field_by_id->get().field_id, 1);
  EXPECT_THAT(field_by_id->get().names, testing::UnorderedElementsAre("id"));
}

}  // namespace iceberg::avro
