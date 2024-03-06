/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <vector>
#include <cstring>
#include "communicator.h"

/**
 * @brief 与客户端进行通讯
 * @ingroup Communicator
 * @details 使用简单的文本通讯协议，每个消息使用'\0'结尾
 */
class PlainCommunicator : public Communicator 
{
public:
  PlainCommunicator();
  ~PlainCommunicator() override = default;

  RC read_event(SessionRequest *&event) override;
  RC write_state(SqlResult *sql_result, bool &need_disconnect) override;
  RC write_result(const char *data, int32_t size) override;
};
