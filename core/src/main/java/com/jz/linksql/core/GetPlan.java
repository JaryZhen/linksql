/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jz.linksql.core;

import com.jz.linksql.core.exec.ApiResult;
import com.jz.linksql.core.exec.ExecuteProcessHelper;
import com.jz.linksql.core.exec.ParamsInfo;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  local模式获取sql任务的执行计划
 * Date: 2020/2/17
 * Company: www.dtstack.com
 * @author maqi
 */
public class GetPlan {

    public static String getExecutionPlan(String[] args) {
        try {
            long start = System.currentTimeMillis();
            ParamsInfo paramsInfo = ExecuteProcessHelper.parseParams(args);
            StreamExecutionEnvironment env = ExecuteProcessHelper.getStreamExecution(paramsInfo);
            String executionPlan = env.getExecutionPlan();
            long end = System.currentTimeMillis();
            System.out.println("get plan=="+ApiResult.createSuccessResultJsonStr(executionPlan, end - start));
            return ApiResult.createSuccessResultJsonStr(executionPlan, end - start);
        } catch (Exception e) {
            System.out.println("get plan=="+ApiResult.createErrorResultJsonStr(ExceptionUtils.getFullStackTrace(e)));
            return ApiResult.createErrorResultJsonStr(ExceptionUtils.getFullStackTrace(e));
        }
    }
}
