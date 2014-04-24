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
package com.snowplowanalytics.snowplow.kinesis.utils;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.autoscaling.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

/**
 * Utilities used to create and delete DynamoDB resources.
 */
public class DynamoDBUtils {

    private static Log LOG = LogFactory.getLog(DynamoDBUtils.class);

    /**
     * Creates the DynamoDB table if it does not already exist and have the correct schema. Then it
     * waits for the table to become active.
     * 
     * @param client
     *        The {@link AmazonDynamoDBClient} with DynamoDB read and write privileges
     * @param tableName
     *        The DynamoDB table to create
     * @param key
     *        The DynamoDB table hashkey
     * @param readCapacityUnits
     *        Number of read capacity units for the DynamoDB table
     * @param writeCapacityUnits
     *        Number of write capacity units for the DynamoDB table
     * @throws IllegalStateException
     *         Table already exists and schema does not match
     * @throws IllegalStateException
     *         Table is already getting created
     */
    public static void createTable(AmazonDynamoDBClient client, String tableName, String key,
            long readCapacityUnits, long writeCapacityUnits) {
        if (tableExists(client, tableName)) {
            if (tableHasCorrectSchema(client, tableName, key)) {
                waitForActive(client, tableName);
                return;
            } else {
                throw new IllegalStateException("Table already exists and schema does not match");
            }

        }
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableName(tableName);
        createTableRequest.setKeySchema(Arrays.asList(new KeySchemaElement(key, KeyType.HASH)));
        createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(readCapacityUnits,
                writeCapacityUnits));
        createTableRequest.setAttributeDefinitions(Arrays.asList(new AttributeDefinition(key,
                ScalarAttributeType.S)));
        try {
            client.createTable(createTableRequest);
        } catch (ResourceInUseException e) {
            throw new IllegalStateException("The table may already be getting created.", e);
        }
        LOG.info("Table " + tableName + " created");
        waitForActive(client, tableName);
    }

    /**
     * Helper method to wait until DynamoDB table becomes active.
     * 
     * @param client
     *        The {@link AmazonDynamoDBClient} with DynamoDB read privileges
     * @param tableName
     *        The DynamoDB table to check the state of
     * @throws IllegalStateException
     *         Table is in the deleting state
     * @throws IllegalStateException
     *         Table did not become active before timeout
     */
    private static void waitForActive(AmazonDynamoDBClient client, String tableName) {
        switch (getTableStatus(client, tableName)) {
        case DELETING:
            throw new IllegalStateException("Table " + tableName + " is in the DELETING state");
        case ACTIVE:
            LOG.info("Table " + tableName + " is ACTIVE");
            return;
        default:
            long startTime = System.currentTimeMillis();
            long endTime = startTime + (10 * 60 * 1000);
            while (System.currentTimeMillis() < endTime) {
                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                }
                try {
                    if (getTableStatus(client, tableName) == TableStatus.ACTIVE) {
                        LOG.info("Table " + tableName + " is ACTIVE");
                        return;
                    }
                } catch (ResourceNotFoundException e) {
                    throw new IllegalStateException("Table " + tableName + " never went active");
                }
            }
        }
    }

    /**
     * Helper method to get the status of a DynamoDB table
     * 
     * @param client
     *        The {@link AmazonDynamoDBClient} with DynamoDB read privileges
     * @param tableName
     *        The DynamoDB table to get the status of
     * @return
     */
    private static TableStatus getTableStatus(AmazonDynamoDBClient client, String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        DescribeTableResult describeTableResult = client.describeTable(describeTableRequest);
        String status = describeTableResult.getTable().getTableStatus();
        return TableStatus.fromValue(status);
    }

    /**
     * Verifies if the table has the expected schema.
     * @param client
     *        The {@link AmazonDynamoDBClient} with DynamoDB read privileges
     * @param tableName
     *        The DynamoDB table to check
     * @param key
     *        The expected hashkey for the DynamoDB table
     * @return true if the DynamoDB table exists and the expected hashkey matches the table schema,
     *         otherwise return false
     */
    private static boolean tableHasCorrectSchema(AmazonDynamoDBClient client, String tableName, String key) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        DescribeTableResult describeTableResult = client.describeTable(describeTableRequest);
        TableDescription tableDescription = describeTableResult.getTable();
        if (tableDescription.getAttributeDefinitions().size() != 1) {
            LOG.error("The number of attribute definitions does not match the existing table.");
            return false;
        }
        AttributeDefinition attributeDefinition = tableDescription.getAttributeDefinitions().get(0);
        if (!attributeDefinition.getAttributeName().equals(key)
                || !attributeDefinition.getAttributeType().equals(ScalarAttributeType.S.toString())) {
            LOG.error("Attribute name or type does not match existing table.");
            return false;
        }
        List<KeySchemaElement> KSEs = tableDescription.getKeySchema();
        if (KSEs.size() != 1) {
            LOG.error("The number of key schema elements does not match the existing table.");
            return false;
        }
        KeySchemaElement kse = KSEs.get(0);
        if (!kse.getAttributeName().equals(key) || !kse.getKeyType().equals(KeyType.HASH.toString())) {
            LOG.error("The hash key does not match the existing table.");
            return false;
        }
        return true;

    }

    /**
     * Helper method to determine if a DynamoDB table exists.
     * 
     * @param client
     *            The {@link AmazonDynamoDBClient} with DynamoDB read privileges
     * @param tableName
     *            The DynamoDB table to check for
     * @return true if the DynamoDB table exists, otherwise return false
     */
    private static boolean tableExists(AmazonDynamoDBClient client, String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        try {
            client.describeTable(describeTableRequest);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    /**
     * Deletes a DynamoDB table if it exists.
     * 
     * @param client
     *            The {@link AmazonDynamoDBClient} with DynamoDB read and write privileges
     * @param tableName
     *            The DynamoDB table to delete
     */
    public static void deleteTable(AmazonDynamoDBClient client, String tableName) {
        if (tableExists(client, tableName)) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
            deleteTableRequest.setTableName(tableName);
            client.deleteTable(deleteTableRequest);
            LOG.info("Deleted table " + tableName);
        } else {
            LOG.warn("Table " + tableName + " does not exist");
        }
    }
}
