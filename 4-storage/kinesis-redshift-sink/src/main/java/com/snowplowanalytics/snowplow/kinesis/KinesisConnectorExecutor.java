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

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.snowplowanalytics.snowplow.kinesis.utils.DynamoDBUtils;
import com.snowplowanalytics.snowplow.kinesis.utils.KinesisUtils;
import com.snowplowanalytics.snowplow.kinesis.utils.RedshiftUtils;
import com.snowplowanalytics.snowplow.kinesis.utils.S3Utils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * This class defines the execution of a Kinesis Connector.
 * 
 */
public abstract class KinesisConnectorExecutor<T,U> extends KinesisConnectorExecutorBase<T,U> {

    // Create AWS Resource constants
    private static final String CREATE_KINESIS_INPUT_STREAM = "createKinesisInputStream";
    private static final String CREATE_KINESIS_OUTPUT_STREAM = "createKinesisOutputStream";
    private static final String CREATE_DYNAMODB_DATA_TABLE = "createDynamoDBDataTable";
    private static final String CREATE_REDSHIFT_CLUSTER = "createRedshiftCluster";
    private static final String CREATE_REDSHIFT_DATA_TABLE = "createRedshiftDataTable";
    private static final String CREATE_REDSHIFT_FILE_TABLE = "createRedshiftFileTable";
    private static final String CREATE_S3_BUCKET = "createS3Bucket";
    private static final boolean DEFAULT_CREATE_RESOURCES = false;

    // Create Kinesis Resource constants
    private static final String KINESIS_INPUT_STREAM_SHARD_COUNT = "kinesisInputStreamShardCount";
    private static final String KINESIS_OUTPUT_STREAM_SHARD_COUNT = "kinesisOutpuStreamShardCount";
    private static final int DEFAULT_KINESIS_SHARD_COUNT = 1;

    // Create DynamoDB Resource constants
    private static final String DYNAMODB_KEY = "dynamoDBKey";
    private static final String DYNAMODB_READ_CAPACITY_UNITS = "readCapacityUnits";
    private static final String DYNAMODB_WRITE_CAPACITY_UNITS = "writeCapacityUnits";
    private static final Long DEFAULT_DYNAMODB_READ_CAPACITY_UNITS = 1l;
    private static final Long DEFAULT_DYNAMODB_WRITE_CAPACITY_UNITS = 1l;

    // Create Redshift Resource constants
    private static final String REDSHIFT_CLUSTER_IDENTIFIER = "redshiftClusterIdentifier";
    private static final String REDSHIFT_DATABASE_NAME = "redshiftDatabaseName";
    private static final String REDSHIFT_CLUSTER_TYPE = "redshiftClusterType";
    private static final String REDSHIFT_NUMBER_OF_NODES = "redshiftNumberOfNodes";
    private static final int DEFAULT_REDSHIFT_NUMBER_OF_NODES = 2;

    // Create S3 Resource constants
    private static final String S3_BUCKET = "s3Bucket";

    // Class variables
    protected final KinesisConnectorConfiguration config;
    private final Properties properties;

    /**
     * Create a new KinesisConnectorExecutor based on the provided configuration (*.propertes) file
     * 
     * @param configFile
     *            The name of the configuration file to look for on the classpath
     */
    public KinesisConnectorExecutor(String configFile) {
        InputStream configStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(configFile);

        if (configStream == null) {
            String msg = "Could not find resource " + configFile + " in the classpath";
            throw new IllegalStateException(msg);
        }
        properties = new Properties();
        try {
            properties.load(configStream);
            configStream.close();
        } catch (IOException e) {
            String msg = "Could not load properties file " + configFile + " from classpath";
            throw new IllegalStateException(msg, e);
        }
        this.config = new KinesisConnectorConfiguration(properties, getAWSCredentialsProvider());
        setupAWSResources();
        
        // Initialize executor with configurations
        super.initialize(config);
    }

    /**
     * Returns an {@link AWSCredentialsProvider} with the permissions necessary to accomplish all specified
     * tasks. At the minimum it will require Kinesis read permissions. Additional read permissions
     * and write permissions may be required based on the Pipeline used
     * 
     * @return
     */
    public AWSCredentialsProvider getAWSCredentialsProvider() {
        return new DefaultAWSCredentialsProviderChain();
    }

    /**
     * Setup necessary AWS resources for the samples. By default, the Executor does not create any
     * AWS resources. The user must specify true for the specific create properties in the
     * configuration file.
     */
    private void setupAWSResources() {
        if (parseBoolean(CREATE_KINESIS_INPUT_STREAM, DEFAULT_CREATE_RESOURCES, properties)) {
            int shardCount = parseInt(KINESIS_INPUT_STREAM_SHARD_COUNT, DEFAULT_KINESIS_SHARD_COUNT,
                    properties);
            KinesisUtils.createInputStream(config, shardCount);
        }
        
        if (parseBoolean(CREATE_KINESIS_OUTPUT_STREAM, DEFAULT_CREATE_RESOURCES, properties)) {
            int shardCount = parseInt(KINESIS_OUTPUT_STREAM_SHARD_COUNT, DEFAULT_KINESIS_SHARD_COUNT,
                    properties);
            KinesisUtils.createOutputStream(config, shardCount);
        }
        
        if (parseBoolean(CREATE_DYNAMODB_DATA_TABLE, DEFAULT_CREATE_RESOURCES, properties)) {
            String key = properties.getProperty(DYNAMODB_KEY);
            Long readCapacityUnits = parseLong(DYNAMODB_READ_CAPACITY_UNITS,
                    DEFAULT_DYNAMODB_READ_CAPACITY_UNITS, properties);
            Long writeCapacityUnits = parseLong(DYNAMODB_WRITE_CAPACITY_UNITS,
                    DEFAULT_DYNAMODB_WRITE_CAPACITY_UNITS, properties);
            createDynamoDBTable(key, readCapacityUnits, writeCapacityUnits);
        }
        
        if (parseBoolean(CREATE_REDSHIFT_CLUSTER, DEFAULT_CREATE_RESOURCES, properties)) {
            String clusterIdentifier = properties.getProperty(REDSHIFT_CLUSTER_IDENTIFIER);
            String databaseName = properties.getProperty(REDSHIFT_DATABASE_NAME);
            String clusterType = properties.getProperty(REDSHIFT_CLUSTER_TYPE);
            int numberOfNodes = parseInt(REDSHIFT_NUMBER_OF_NODES, DEFAULT_REDSHIFT_NUMBER_OF_NODES,
                    properties);
            createRedshiftCluster(clusterIdentifier, databaseName, clusterType, numberOfNodes);
        }
        
        if (parseBoolean(CREATE_REDSHIFT_DATA_TABLE, DEFAULT_CREATE_RESOURCES, properties)) {
            createRedshiftDataTable();
        }
        
        if (parseBoolean(CREATE_REDSHIFT_FILE_TABLE, DEFAULT_CREATE_RESOURCES, properties)) {
            createRedshiftFileTable();
        }
        
        if (parseBoolean(CREATE_S3_BUCKET, DEFAULT_CREATE_RESOURCES, properties)) {
            String s3Bucket = properties.getProperty(S3_BUCKET);
            createS3Bucket(s3Bucket);
        }
    }

    /**
     * Helper method to create the DynamoDB table
     * 
     * @param key
     *            The name of the hashkey field in the DynamoDB table
     * @param readCapacityUnits
     *            Read capacity of the DynamoDB table
     * @param writeCapacityUnits
     *            Write capacity of the DynamoDB table
     */
    private void createDynamoDBTable(String key, long readCapacityUnits, long writeCapacityUnits) {
        DynamoDBUtils.createTable(new AmazonDynamoDBClient(config.AWS_CREDENTIALS_PROVIDER),
                config.DYNAMODB_DATA_TABLE_NAME, key, readCapacityUnits, writeCapacityUnits);
    }

    /**
     * Helper method to create the Redshift cluster
     * 
     * @param clusterIdentifier
     *            Unique identifier for the name of the Redshift cluster
     * @param databaseName
     *            Name for the database in the Redshift cluster
     * @param clusterType
     *            dw.hs1.xlarge or dw.hs1.8xlarge
     * @param numberOfNodes
     *            Number of nodes for the Redshift cluster
     */
    private void createRedshiftCluster(String clusterIdentifier, String databaseName, String clusterType,
            int numberOfNodes) {
        // Make sure Redshift cluster is available
        AmazonRedshiftClient redshiftClient = new AmazonRedshiftClient(config.AWS_CREDENTIALS_PROVIDER);
        redshiftClient.setEndpoint(config.REDSHIFT_ENDPOINT);
        RedshiftUtils.createCluster(redshiftClient, clusterIdentifier, databaseName,
                config.REDSHIFT_USERNAME, config.REDSHIFT_PASSWORD, clusterType, numberOfNodes);

        // Update Redshift connection url
        config.REDSHIFT_URL = RedshiftUtils.getClusterURL(redshiftClient, clusterIdentifier);
    }

    /**
     * Helper method to create the data table in Redshift
     *
     * @deprecated
     */
    @Deprecated
    private void createRedshiftDataTable() {
        Properties p = new Properties();
        p.setProperty("user", config.REDSHIFT_USERNAME);
        p.setProperty("password", config.REDSHIFT_PASSWORD);
        if (RedshiftUtils.tableExists(p, config.REDSHIFT_URL, config.REDSHIFT_DATA_TABLE)) {
            return;
        }
        try {
            RedshiftUtils.createRedshiftTable(config.REDSHIFT_URL, p, config.REDSHIFT_DATA_TABLE,
                    getKinesisMessageModelFields());
        } catch (SQLException e) {
            String msg = "Could not create Redshift data table " + config.REDSHIFT_DATA_TABLE;
            throw new IllegalStateException(msg, e);
        }
    }

    /**
     * Helper method to create the file table in Redshift
     */
    private void createRedshiftFileTable() {
        Properties p = new Properties();
        p.setProperty("user", config.REDSHIFT_USERNAME);
        p.setProperty("password", config.REDSHIFT_PASSWORD);
        if (RedshiftUtils.tableExists(p, config.REDSHIFT_URL, config.REDSHIFT_FILE_TABLE)) {
            return;
        }
        try {
            RedshiftUtils.createRedshiftTable(config.REDSHIFT_URL, p, config.REDSHIFT_FILE_TABLE,
                    getFileTableFields());
        } catch (SQLException e) {
            String msg = "Could not create Redshift file table " + config.REDSHIFT_FILE_TABLE;
            throw new IllegalStateException(msg, e);
        }
    }

    /**
     * Helper method to build the data table
     * 
     * @return Fields for the data table
     *
     * @deprecated
     */
    @Deprecated
    private static List<String> getKinesisMessageModelFields() {
        List<String> fields = new ArrayList<String>();
        fields.add("userid integer not null distkey sortkey");
        fields.add("username char(8)");
        fields.add("firstname varchar(30)");
        fields.add("lastname varchar(30)");
        fields.add("city varchar(30)");
        fields.add("state char(2)");
        fields.add("email varchar(100)");
        fields.add("phone char(14)");
        fields.add("likesports boolean");
        fields.add("liketheatre boolean");
        fields.add("likeconcerts boolean");
        fields.add("likejazz boolean");
        fields.add("likeclassical boolean");
        fields.add("likeopera boolean");
        fields.add("likerock boolean");
        fields.add("likevegas boolean");
        fields.add("likebroadway boolean");
        fields.add("likemusicals boolean");
        return fields;
    }

    /**
     * Helper method to help create the file table
     * 
     * @return File table fields
     */
    private static List<String> getFileTableFields() {
        List<String> fields = new ArrayList<String>();
        fields.add("file varchar(255) primary key");
        return fields;
    }

    /**
     * Helper method to create the S3Bucket
     * 
     * @param s3Bucket
     *            The name of the bucket to create
     */
    private void createS3Bucket(String s3Bucket) {
        AmazonS3Client client = new AmazonS3Client(config.AWS_CREDENTIALS_PROVIDER);
        client.setEndpoint(config.S3_ENDPOINT);
        S3Utils.createBucket(client, s3Bucket);
    }

    /**
     * Helper method used to parse boolean properties
     * 
     * @param property
     *            The String key for the property
     * @param defaultValue
     *            The default value for the boolean property
     * @param properties
     *            The properties file to get property from
     * @return property from property file, or if it is not specified, the default value
     */
    private static boolean parseBoolean(String property, boolean defaultValue, Properties properties) {
        return Boolean.parseBoolean(properties.getProperty(property, Boolean.toString(defaultValue)));
    }

    /**
     * Helper method used to parse long properties
     * 
     * @param property
     *            The String key for the property
     * @param defaultValue
     *            The default value for the long property
     * @param properties
     *            The properties file to get property from
     * @return property from property file, or if it is not specified, the default value
     */
    private static long parseLong(String property, long defaultValue, Properties properties) {
        return Long.parseLong(properties.getProperty(property, Long.toString(defaultValue)));
    }

    /**
     * Helper method used to parse integer properties
     * 
     * @param property
     *            The String key for the property
     * @param defaultValue
     *            The default value for the integer property
     * @param properties
     *            The properties file to get property from
     * @return property from property file, or if it is not specified, the default value
     */
    private static int parseInt(String property, int defaultValue, Properties properties) {
        return Integer.parseInt(properties.getProperty(property, Integer.toString(defaultValue)));
    }
}
