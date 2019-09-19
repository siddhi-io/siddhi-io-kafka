/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.io.kafka;

import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Util class of Kafka IO.
 */
public class KafkaIOUtils {

    public static final String HEADER_SEPARATOR = ",";
    private static final String ENTRY_SEPARATOR = ":";
    private static final Logger LOG = Logger.getLogger(KafkaIOUtils.class);

    public static void splitHeaderValues(String optionalConfigs, Properties configProperties) {
        if (optionalConfigs != null && !optionalConfigs.isEmpty()) {
            String[] optionalProperties = optionalConfigs.split(HEADER_SEPARATOR);
            if (optionalProperties.length > 0) {
                for (String header : optionalProperties) {
                    try {
                        String[] configPropertyWithValue = header.split(ENTRY_SEPARATOR, 2);
                        configProperties.put(configPropertyWithValue[0], configPropertyWithValue[1]);
                    } catch (Exception e) {
                        LOG.warn("Optional property '" + header + "' is not defined in the correct format.",
                                e);
                    }
                }
            }
        }
    }
}
