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



package com.jz.linksql.core.side;

import com.jz.linksql.core.enums.ECacheContentType;
import com.jz.linksql.core.side.cache.CacheObj;

/**
 * Only the data marked to dimension table miss
 * Date: 2021/8/28
 *
 * @author xuchao
 */

public class CacheMissVal {

    private static CacheObj missObj = CacheObj.buildCacheObj(ECacheContentType.MissVal, null);

    public static CacheObj getMissKeyObj(){
        return missObj;
    }
}
