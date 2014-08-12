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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.Region;

public class S3Utils {

    private static Log LOG = LogFactory.getLog(S3Utils.class);

    /**
     * Create a S3 bucket if it does not exist.
     * 
     * @param client
     *            The {@link AmazonS3Client} with read and write permissions
     * @param bucketName
     *            The bucket to create
     * @throws IllegalStateException
     *             The bucket is not created before timeout occurs
     */
    public static void createBucket(AmazonS3Client client, String bucketName) {
        if (!bucketExists(client, bucketName)) {
            CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName);
            createBucketRequest.setRegion(Region.US_Standard.toString());
            client.createBucket(createBucketRequest);
        }
        long startTime = System.currentTimeMillis();
        long endTime = startTime + 60 * 1000;
        while (!bucketExists(client, bucketName) && endTime > System.currentTimeMillis()) {
            try {
                LOG.info("Waiting for S3 to create bucket " + bucketName);
                Thread.sleep(1000 * 10);
            } catch (InterruptedException e) {
            }
        }
        if (!bucketExists(client, bucketName)) {
            throw new IllegalStateException("Could not create bucket " + bucketName);
        }
        LOG.info("Created S3 bucket " + bucketName);
    }

    /**
     * 
     * @param client
     *            The {@link AmazonS3Client} with read permissions
     * @param bucketName
     *            Check if this bucket exists
     * @return true if the S3 bucket exists, otherwise return false
     */
    private static boolean bucketExists(AmazonS3Client client, String bucketName) {
        return client.doesBucketExist(bucketName);
    }

    /**
     * Deletes a S3 bucket if it exists.
     * @param client The {@link AmazonS3Client} with read and write permissions
     * @param bucketName The S3 bucket to delete
     */
    public static void deleteBucket(AmazonS3Client client, String bucketName) {
        if (bucketExists(client, bucketName)) {
            DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
            client.deleteBucket(deleteBucketRequest);
            LOG.info("Deleted bucket " + bucketName);
        } else {
            LOG.warn("Bucket " + bucketName + " does not exist");
        }
    }
}
