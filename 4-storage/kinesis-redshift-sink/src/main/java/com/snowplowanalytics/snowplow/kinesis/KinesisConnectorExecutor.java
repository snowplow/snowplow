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
     * Supported data types: http://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
     * Snowplow cannonical event model code: https://github.com/snowplow/snowplow/blob/c4189163f71ec99215485eff72398ca7bfc3556d/3-enrich/scala-common-enrich/src/main/scala/com.snowplowanalytics.snowplow.enrich/common/outputs/EnrichedEvent.scala#L39
     * Snowplow cannonical event model descriptions/types: https://github.com/snowplow/snowplow/wiki/Canonical-event-model
     *
     * @deprecated
     */
    @Deprecated
    private static List<String> getKinesisMessageModelFields() {
        List<String> fields = new ArrayList<String>();
        // The application (site, game, app etc) this event belongs to, and the tracker platform
        fields.add("app_id varchar(30)"); // Ex: "force"
        fields.add("platform varchar(30)"); // Ex: "web"
        // Date/time
        fields.add("etl_tstamp timestamp"); // Ex: "2014-09-18 15:50:33.333"
        fields.add("collector_tstamp timestamp"); // Ex: "2014-09-18 15:50:15.156"
        // Transaction (i.e. this logging event)
        fields.add("event varchar(30)"); // Ex: "struct"
        fields.add("name_org varchar(30)"); // Ex: "com.snowplowanalytics"
        fields.add("event_id char(36)"); // Ex: "4b25d258-2831-4fdc-8125-43ebf2d5b12b"
        fields.add("txn_id bigint"); // Ex: "644474"
        // Versioning
        fields.add("v_tracker varchar(30)"); // Ex: "js-2.0.0"
        fields.add("v_collector varchar(30)"); // Ex: "ssc-0.1.0-kinesis"
        fields.add("v_etl varchar(30)"); // Ex: "kinesis-0.1.0-common-0.2.0"
        // User and visit
        fields.add("user_id char(24)"); // Ex: "541afb78636172313c000000"
        fields.add("user_ipaddress varchar(30)"); // Ex: "108.27.195.x"
        fields.add("user_fingerprint bigint"); // Ex: "4135552504"
        fields.add("domain_userid char(16)"); // Ex: "7940bcc0dffc1363"
        fields.add("domain_sessionidx bigint"); // Ex: "1"
        fields.add("network_userid char(36)"); // Ex: ""
        // Location
        fields.add("geo_country char(2)"); // Ex: "US"
        fields.add("geo_region char(2)"); // Ex: "NY"
        fields.add("geo_city varchar(120)"); // Ex: "New York"
        fields.add("geo_zipcode varchar(30)"); // Ex: "10023"
        fields.add("geo_latitude varchar(30)"); // Ex: "40.7769"
        fields.add("geo_longitude varchar(30)"); // Ex: "-73.9813"
        fields.add("geo_region_name varchar(120)"); // Ex: ""
        // Other IP lookups SKIPPED
        // Page SKIPPED
        // Page URL components
        fields.add("page_urlscheme varchar(8)"); // Ex: "http"
        fields.add("page_urlhost varchar(120)"); // Ex: "localhost"
        fields.add("page_urlport integer"); // Ex: "5000"
        fields.add("page_urlpath varchar(2000)"); // Ex: "/artwork/ben-jones-cubes"
        fields.add("page_urlquery varchar(2000)"); // Ex: ""
        fields.add("page_urlfragment varchar(2000)"); // Ex: ""
        // Referrer URL components
        fields.add("refr_urlscheme varchar(8)"); // Ex: "http"
        fields.add("refr_urlhost varchar(120)"); // Ex: "localhost"
        fields.add("refr_urlport integer"); // Ex: "5000"
        fields.add("refr_urlpath varchar(2000)"); // Ex: "/"
        fields.add("refr_urlquery varchar(2000)"); // Ex: ""
        fields.add("refr_urlfragment varchar(2000)"); // Ex: ""
        // Referrer details
        fields.add("refr_medium varchar(30)"); // Ex: "internal"
        fields.add("refr_source varchar(120)"); // Ex: ""
        fields.add("refr_term varchar(260)"); // Ex: ""
        // Marketing
        fields.add("mkt_medium varchar(120)"); // Ex: 'social'
        fields.add("mkt_source varchar(120)"); // Ex: 'Facebook'
        fields.add("mkt_term varchar(120)"); // Ex: 'new age tarot decks'
        fields.add("mkt_content varchar(120)"); // Ex: 13894723
        fields.add("mkt_campaign varchar(120)"); // Ex: 'diageo-123'
        // Custom Contexts SKIPPED
        // Structured Event
        fields.add("se_category varchar(30)"); // Ex: "inquiry"
        fields.add("se_action varchar(30)"); // Ex: "submit"
        fields.add("se_label varchar(120)"); // Ex: "541afb78636172313c000000"
        fields.add("se_property varchar(30)"); // Ex: "artwork"
        fields.add("se_value real"); // Ex: "0.0"
        // Unstructured Event SKIPPED
        // Ecommerce transaction (from querystring)
        fields.add("tr_orderid varchar(30)"); //  Ex: '#134'
        fields.add("tr_affiliation varchar(30)"); //  Ex: 'web'
        fields.add("tr_total real"); //  Ex: 12.99
        fields.add("tr_tax real"); //  Ex: 3.00
        fields.add("tr_shipping real"); //  Ex: 0.00
        fields.add("tr_city varchar(30)"); //  Ex: 'London'
        fields.add("tr_state varchar(30)"); //  Ex: 'Washington'
        fields.add("tr_country varchar(30)"); //  Ex: 'France'
        // Ecommerce transaction item (from querystring)
        fields.add("ti_orderid varchar(30)"); // Ex: '#134'
        fields.add("ti_sku varchar(30)"); // Ex: 'pbz00123'
        fields.add("ti_name varchar(30)"); // Ex: Cone 'pendulum'
        fields.add("ti_category varchar(30)"); // Ex: 'New Age'
        fields.add("ti_price varchar(30)"); // Ex: '9.99'
        fields.add("ti_quantity varchar(30)"); // Ex: '2'
        // Page Pings
        fields.add("pp_xoffset_min integer"); // Ex: 0
        fields.add("pp_xoffset_max integer"); // Ex: 100
        fields.add("pp_yoffset_min integer"); // Ex: 0
        fields.add("pp_yoffset_max integer"); // Ex: 200
        // User Agent
        fields.add("useragent varchar(2000)"); // Ex: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36"
        // Browser (from user-agent)
        fields.add("br_name varchar(120)"); // Ex: "Chrome"
        fields.add("br_family varchar(120)"); // Ex: "Chrome"
        fields.add("br_version varchar(120)"); // Ex: "37.0.2062.120"
        fields.add("br_type varchar(120)"); // Ex: "Browser"
        fields.add("br_renderengine varchar(120)"); // Ex: "WEBKIT"
        // Browser (from querystring)
        fields.add("br_lang varchar(30)"); // Ex: "en-US"
        // Individual feature fields for non-Hive targets (e.g. Infobright)
        fields.add("br_features_pdf boolean"); // Ex: "1"
        fields.add("br_features_flash boolean"); // Ex: "1"
        fields.add("br_features_java boolean"); // Ex: "1"
        fields.add("br_features_director boolean"); // Ex: "0"
        fields.add("br_features_quicktime boolean"); // Ex: "1"
        fields.add("br_features_realplayer boolean"); // Ex: "0"
        fields.add("br_features_windowsmedia boolean"); // Ex: "0"
        fields.add("br_features_gears boolean"); // Ex: "0"
        fields.add("br_features_silverlight boolean"); // Ex: "1"
        fields.add("br_cookies boolean"); // Ex: "1"
        fields.add("br_colordepth integer"); // Ex: "24"
        fields.add("br_viewwidth integer"); // Ex: "1680"
        fields.add("br_viewheight integer"); // Ex: "517"
        // OS (from user-agent)
        fields.add("os_name varchar(120)"); // Ex: "Mac OS X"
        fields.add("os_family varchar(120)"); // Ex: "Mac OS X"
        fields.add("os_manufacturer varchar(120)"); // Ex: "Apple Inc."
        fields.add("os_timezone varchar(120)"); // Ex: "America/New_York"
        // Device/Hardware (from user-agent)
        fields.add("dvce_type varchar(30)"); // Ex: "Computer"
        fields.add("dvce_ismobile boolean"); // Ex: "0"
        // Device (from querystring)
        fields.add("dvce_screenwidth integer"); // Ex: "1680"
        fields.add("dvce_screenheight integer"); // Ex: "1050"
        // Document
        fields.add("doc_charset varchar(30)"); // Ex: "UTF-8"
        fields.add("doc_width integer"); // Ex: "1680"
        fields.add("doc_height integer"); // Ex: "1293"
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