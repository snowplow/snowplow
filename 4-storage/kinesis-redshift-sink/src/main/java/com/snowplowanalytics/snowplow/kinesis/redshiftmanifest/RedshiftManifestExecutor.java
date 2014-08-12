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
 * The executor used to read S3 file names from the secondary Kinesis stream, create a manifest file
 * in S3, and then perform a Redshift manifest copy.
 */
public class RedshiftManifestExecutor extends KinesisConnectorExecutor<String, String> {

    /**
     * Creates a new RedshiftManifestExecutor.
     * 
     * @param configFile
     *            The name of the configuration file to look for on the classpath.
     */
    public RedshiftManifestExecutor(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<String, String> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(new RedshiftManifestPipeline(), config);
    }

    private static String REDSHIFT_CONFIG_FILE = "RedshiftManifest.properties";
    private static String S3_CONFIG_FILE = "S3Manifest.properties";

    /**
     * Main method to run the Redshift manifest copy sample. Runs both the {@link S3ManifestExecutor} and
     * {@link RedshiftManifestExecutor}
     * 
     * @param args
     */
    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not load PostgreSQL driver");
        }
        // Spawn the S3Executor
        KinesisConnectorExecutor<String, byte[]> s3ManifestExecutor = new S3ManifestExecutor(
                S3_CONFIG_FILE);
        Thread s3Thread = new Thread(s3ManifestExecutor);
        s3Thread.start();

        // Run the RedshiftExecutor
        KinesisConnectorExecutor<String, String> redshiftExecutor = new RedshiftManifestExecutor(REDSHIFT_CONFIG_FILE);
        redshiftExecutor.run();
    }
}
