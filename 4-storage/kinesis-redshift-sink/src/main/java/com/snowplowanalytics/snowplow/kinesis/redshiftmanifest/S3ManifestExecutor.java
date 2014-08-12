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
package com.snowplowanalytics.snowplow.kinesis.redshiftmanifest;

import com.snowplowanalytics.snowplow.kinesis.KinesisConnectorExecutor;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;

/**
 * The executor used to emit files to S3 and then put the file names to a secondary Kinesis stream.
 */
public class S3ManifestExecutor extends KinesisConnectorExecutor<String, byte[]> {

    /**
     * Creates a new S3ManifestExecutor.
     * 
     * @param configFile
     *            The name of the configuration file to look for on the classpath
     */
    public S3ManifestExecutor(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<String, byte[]> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(new S3ManifestPipeline(), config);
    }
}
