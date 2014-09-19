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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.snowplowanalytics.snowplow.kinesis.MyS3Emitter;

/**
 * This class is an implementation of IEmitter that emits records into Redshift one by one. It
 * utilizes the Redshift copy command on each file by first inserting records into S3 and then
 * performing the Redshift copy command. S3 insertion is done by extending the S3 emitter.
 * <p>
 * * This class requires the configuration of an S3 bucket and endpoint, as well as the following
 * Redshift items:
 * <ul>
 * <li>Redshift URL</li>
 * <li>username and password</li>
 * <li>data table and key column (data table stores items from the manifest copy)</li>
 * <li>file table and key column (file table is used to store file names to prevent duplicate
 * entries)</li>
 * <li>the delimiter used for string parsing when inserting entries into Redshift</li>
 * <br>
 * NOTE: The S3 bucket and the Redshift cluster need to be in the same region.
 */
public class MyRedshiftBasicEmitter extends MyS3Emitter {
    private static final Log LOG = LogFactory.getLog(MyRedshiftBasicEmitter.class);
    private final String s3bucket;
    private final String redshiftTable;
    private final String redshiftURL;
    private final char redshiftDelimiter;
    private final Properties loginProperties;
    private final String accessKey;
    private final String secretKey;

    public MyRedshiftBasicEmitter(KinesisConnectorConfiguration configuration) {
        super(configuration);
        s3bucket = configuration.S3_BUCKET;
        redshiftTable = configuration.REDSHIFT_DATA_TABLE;
        redshiftDelimiter = configuration.REDSHIFT_DATA_DELIMITER;
        redshiftURL = configuration.REDSHIFT_URL;
        loginProperties = new Properties();
        loginProperties.setProperty("user", configuration.REDSHIFT_USERNAME);
        loginProperties.setProperty("password", configuration.REDSHIFT_PASSWORD);
        accessKey = configuration.AWS_CREDENTIALS_PROVIDER.getCredentials().getAWSAccessKeyId();
        secretKey = configuration.AWS_CREDENTIALS_PROVIDER.getCredentials().getAWSSecretKey();
    }

    @Override
    public List<byte[]> emit(final UnmodifiableBuffer<byte[]> buffer) throws IOException {
        List<byte[]> failed = super.emit(buffer);
        if (!failed.isEmpty()) {
            return buffer.getRecords();
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(redshiftURL, loginProperties);
            String s3File = getS3FileName(buffer.getFirstSequenceNumber(), buffer.getLastSequenceNumber());
            executeStatement(generateCopyStatement(s3File), conn);
            LOG.info("Successfully copied " + getNumberOfCopiedRecords(conn)
                    + " records to Redshift from file s3://" + s3Bucket + "/" + s3File);
            closeConnection(conn);
            return Collections.emptyList();
        } catch (IOException | SQLException e) {
            LOG.error(e);
            closeConnection(conn);
            return buffer.getRecords();
        }
    }

    @Override
    public void fail(List<byte[]> records) {
        super.fail(records);
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    private void closeConnection(Connection conn) {
        try {
            conn.close();
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    private String generateCopyStatement(String s3File) {
        StringBuilder exec = new StringBuilder();
        exec.append("COPY " + redshiftTable + " ");
        exec.append("FROM 's3://" + s3bucket + "/" + s3File + "' ");
        exec.append("CREDENTIALS 'aws_access_key_id=" + accessKey);
        exec.append(";aws_secret_access_key=" + secretKey + "' ");
        exec.append("DELIMITER '" + redshiftDelimiter + "'");
        exec.append(";");
        return exec.toString();
    }

    private void executeStatement(String statement, Connection conn) throws IOException {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(statement);
            stmt.close();
            return;
        } catch (SQLException e) {
            LOG.error(e);
            throw new IOException(e);
        }

    }

    private int getNumberOfCopiedRecords(Connection conn) throws IOException {
        String cmd = "select pg_last_copy_count();";
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(cmd);
            resultSet.next();
            int numCopiedRecords = resultSet.getInt(1);
            resultSet.close();
            stmt.close();
            return numCopiedRecords;
        } catch (SQLException e) {
            try {
                resultSet.close();
            } catch (Exception e1) {
            }
            try {
                stmt.close();
            } catch (Exception e1) {
            }
            throw new IOException(e);
        }

    }

}
