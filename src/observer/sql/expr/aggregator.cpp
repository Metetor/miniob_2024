/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  //add函数的规则是left(1) + right(2) = result(3)
  //Sum聚合函数就是将之前结果加上当前的value赋值给value_
  Value::add(value, value_, value_);
  return RC::SUCCESS;
}

//evaluate将计算结果传递给result
RC SumAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

//实现here
RC CountAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  //Count将value_ 加一
  Value::add(Value(1), value_, value_);
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

//Avg只需要执行Sum和Count中的操作
//最后在evaluate中调用div
RC AvgAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  Value::add(value, value_, value_);
  Value::add(Value(1),value_cnt_,value_cnt_);
  return RC::SUCCESS;
}

//result=value_/value_cnt_
RC AvgAggregator::evaluate(Value& result)
{
  //result需要先设置成float
  result.set_type(AttrType::FLOATS);
  Value::divide(value_,value_cnt_,result);
  return RC::SUCCESS;
}

RC MaxAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  //call Value::compare(other) /// @return 1:大于;-1：小于;0：等于;INT32_MAX:未实现的比较
  value_=value.compare(value_)==1?value:value_;

  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  //call Value::compare(other) /// @return 1:大于;-1：小于;0：等于;INT32_MAX:未实现的比较
  value_=value.compare(value_)==-1?value:value_;

  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}