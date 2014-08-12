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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.ClusterNotFoundException;
import com.amazonaws.services.redshift.model.CreateClusterRequest;
import com.amazonaws.services.redshift.model.DeleteClusterRequest;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.DescribeClustersResult;
import com.amazonaws.services.redshift.model.Endpoint;

public class RedshiftUtils {

    private static Log LOG = LogFactory.getLog(RedshiftUtils.class);

    /**
     * Creates a Redshift cluster if it does not exist and waits for it to become active
     * 
     * @param client
     *            The {@link AmazonRedshiftClient} with read and write permissions
     * @param clusterIdentifier
     *            The unique identifier of the Redshift cluster to create
     * @param databaseName
     *            The database name associated with the Redshift cluster
     * @param masterUsername
     *            The master username for the Redshift database
     * @param masterUserPassword
     *            The master password for the Redshift database
     * @param clusterType
     *            dw.hs1.xlarge or dw.hs1.8xlarge
     * @param numberOfNodes
     *            The number of nodes for the Redshift cluster
     */
    public static void createCluster(AmazonRedshiftClient client, String clusterIdentifier,
            String databaseName, String masterUsername, String masterUserPassword, String clusterType,
            int numberOfNodes) {
        if (clusterExists(client, clusterIdentifier)) {
            LOG.info("Cluster " + clusterIdentifier + " is available");
            return;
        }
        CreateClusterRequest createClusterRequest = new CreateClusterRequest();
        createClusterRequest.setClusterIdentifier(clusterIdentifier);
        createClusterRequest.setMasterUsername(masterUsername);
        createClusterRequest.setMasterUserPassword(masterUserPassword);
        createClusterRequest.setNodeType(clusterType);
        createClusterRequest.setNumberOfNodes(numberOfNodes);
        createClusterRequest.setDBName(databaseName);
        client.createCluster(createClusterRequest);
        LOG.info("Created cluster " + clusterIdentifier);
        String state = null;
        while (!(state = clusterState(client, clusterIdentifier)).equals("available")) {
            try {
                Thread.sleep(10 * 1000);
                LOG.info(clusterIdentifier + " is " + state + ". Waiting for cluster to become available.");
            } catch (InterruptedException e) {

            }
        }
        LOG.info("Cluster " + clusterIdentifier + " is available");
    }

    /**
     * Gets the JDBC URL associated with an active Redshift cluster.
     * 
     * @param client
     *            The {@link AmazonRedshiftClient} with read permissions
     * @param clusterIdentifier
     *            The unique Redshift cluster identifier
     * @return JDBC URL for the Redshift cluster
     */
    public static String getClusterURL(AmazonRedshiftClient client, String clusterIdentifier) {
        DescribeClustersRequest describeClustersRequest = new DescribeClustersRequest();
        describeClustersRequest.setClusterIdentifier(clusterIdentifier);
        DescribeClustersResult describeClustersResult = client.describeClusters(describeClustersRequest);
        List<Cluster> clusters = describeClustersResult.getClusters();
        if (!clusters.isEmpty()) {
            return toJDBC(clusters.get(0).getEndpoint(), clusters.get(0).getDBName());
        }
        return null;
    }

    /**
     * Helper method to convert a Redshift {@link Endpoint} and database name to JDBC connection
     * String
     * 
     * @param endpoint
     *            The Redshit Endpoint to convert to connection String
     * @param databaseName
     *            The database name for the Redshift cluster
     * @return The JDBC connection String associated with the Endpoint and database name
     */
    private static String toJDBC(Endpoint endpoint, String databaseName) {
        StringBuilder jdbc = new StringBuilder();
        jdbc.append("jdbc:postgresql://");
        jdbc.append(endpoint.getAddress());
        jdbc.append(":" + endpoint.getPort());
        jdbc.append("/" + databaseName);
        return jdbc.toString();
    }

    /**
     * Delete the Redshift cluster if it exists
     * 
     * @param client
     *            The {@link AmazonRedshiftClient} with read and write permissions
     * @param clusterIdentifier
     *            The Redshift cluster delete
     * @param skipFinalClusterSnapshot
     *            Should Redshift skip the final cluster snapshot?
     */
    public static void deleteCluster(AmazonRedshiftClient client, String clusterIdentifier,
            boolean skipFinalClusterSnapshot) {
        if (clusterExists(client, clusterIdentifier)) {
            DeleteClusterRequest deleteClusterRequest = new DeleteClusterRequest();
            deleteClusterRequest.setClusterIdentifier(clusterIdentifier);
            deleteClusterRequest.setSkipFinalClusterSnapshot(skipFinalClusterSnapshot);
            client.deleteCluster(deleteClusterRequest);
        } else {
            LOG.warn("Redshift cluster " + clusterIdentifier + " does not exist");
        }
    }

    /**
     * Helper method to determine if a Redshift cluster exists
     * 
     * @param client
     *            The {@link AmazonRedshiftClient} with read permissions
     * @param clusterIdentifier
     *            The Redshift cluster to check
     * @return true if the Redshift cluster exists, otherwise return false
     */
    private static boolean clusterExists(AmazonRedshiftClient client, String clusterIdentifier) {
        DescribeClustersRequest describeClustersRequest = new DescribeClustersRequest();
        describeClustersRequest.setClusterIdentifier(clusterIdentifier);
        try {
            client.describeClusters(describeClustersRequest);
            return true;
        } catch (ClusterNotFoundException e) {
            return false;
        }

    }

    /**
     * Helper method to determine the Redshift cluster state
     * 
     * @param client
     *            The {@link AmazonRedshiftClient} with read permissions
     * @param clusterIdentifier
     *            The Redshift cluster to get the state of
     * @return The String representation of the Redshift cluster state
     */
    public static String clusterState(AmazonRedshiftClient client, String clusterIdentifier) {
        DescribeClustersRequest describeClustersRequest = new DescribeClustersRequest();
        describeClustersRequest.setClusterIdentifier(clusterIdentifier);
        List<Cluster> clusters = client.describeClusters(describeClustersRequest).getClusters();
        if (clusters.size() == 1) {
            return clusters.get(0).getClusterStatus();
        }
        throw new ClusterNotFoundException(clusterIdentifier);

    }

    /**
     * Helper method to create a Redshift table
     * 
     * @param redshiftURL
     *            The JDBC URL of the Redshift database
     * @param loginProperties
     *            A properties file containing the authentication credentials for the database
     * @param tableName
     *            The table to create
     * @param fields
     *            A list of column specifications that will be comma separated in the create table
     *            statement
     * @throws SQLException
     *             Table creation failed
     */
    public static void createRedshiftTable(String redshiftURL, Properties loginProperties, String tableName,
            List<String> fields) throws SQLException {
        Connection conn = DriverManager.getConnection(redshiftURL, loginProperties);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + tableName + " " + toSQLFields(fields) + ";");
        stmt.close();
        conn.close();
    }

    /**
     * Helper method to build a field String for creating the table in the format (field1, field2,
     * ...)
     * 
     * @param fields
     * @return String in the format (field1, field2, ...)
     */
    private static String toSQLFields(List<String> fields) {
        StringBuilder s = new StringBuilder();
        s.append("(");
        for (String field : fields) {
            s.append(field);
            s.append(",");
        }
        s.replace(s.length() - 1, s.length(), "");
        s.append(")");
        return s.toString();
    }

    /**
     * Helper method to determine if a table exists in the Redshift database
     * 
     * @param loginProperties
     *            A properties file containing the authentication credentials for the database
     * @param redshiftURL
     *            The JDBC URL of the Redshift database
     * @param tableName
     *            The table to check existence of
     * @return true if connection to the database is successful and the table exists, otherwise
     *         false
     */
    public static boolean tableExists(Properties loginProperties, String redshiftURL, String tableName) {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(redshiftURL, loginProperties);

            Statement stmt = conn.createStatement();
            stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 1;");
            stmt.close();
            conn.close();
            return true;
        } catch (SQLException e) {
            LOG.error(e);
            try {
                conn.close();
            } catch (Exception e1) {
            }
            return false;
        }
    }

}
