/*
 * Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.snowplowanalytics.snowplow.kinesis;

import com.amazonaws.services.kinesis.metrics.impl.CWMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

/**
 * This class defines the execution of a Kinesis Connector with CloudWatch metrics.
 * 
 */
public abstract class KinesisConnectorMetricsExecutor<T,U> extends KinesisConnectorExecutor<T,U> {

    /**
     * Creates a new KinesisConnectorMetricsExecutor
     * @param configFile The name of the configuration file to look for on the classpath
     */
    public KinesisConnectorMetricsExecutor(String configFile) {
        super(configFile);
        
        // CloudWatch Metrics Factory used to emit metrics in KCL
        IMetricsFactory mFactory = new CWMetricsFactory(config.AWS_CREDENTIALS_PROVIDER,
                config.CLOUDWATCH_NAMESPACE, config.CLOUDWATCH_BUFFER_TIME, config.CLOUDWATCH_MAX_QUEUE_SIZE);
        super.initialize(config, mFactory);
    }
}
