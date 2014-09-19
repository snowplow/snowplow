package com.snowplowanalytics.snowplow.kinesis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.s3.AmazonS3Client;

import com.amazonaws.services.kinesis.connectors.s3.S3Emitter;

public class MyS3Emitter extends S3Emitter {
	private static final Log LOG = LogFactory.getLog(S3Emitter.class);

	public MyS3Emitter(KinesisConnectorConfiguration configuration) {
        super(configuration);
    }

	@Override
    public List<byte[]> emit(final UnmodifiableBuffer<byte[]> buffer) throws IOException {
        List<byte[]> records = buffer.getRecords();
        // Write all of the records to a compressed output stream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (byte[] record : records) {
            try {
                baos.write(record);
            } catch (IOException e) {
                LOG.error(e);
                return buffer.getRecords();
            }
        }
        // Get the S3 filename
        String s3FileName = getS3FileName(buffer.getFirstSequenceNumber(), buffer.getLastSequenceNumber());
        ByteArrayInputStream object = new ByteArrayInputStream(baos.toByteArray());
        try {
            s3client.putObject(s3Bucket, s3FileName, object, null);
            LOG.info("Successfully emitted " + buffer.getRecords().size() + " records to S3 in s3://"
                    + s3Bucket + "/" + s3FileName);
            return Collections.emptyList();
        } catch (AmazonServiceException e) {
            LOG.error(e);
            return buffer.getRecords();
        }
    }
}