package com.digdeep.util.logging;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import scalaz.std.stream;

import java.io.*;
import java.text.MessageFormat;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.zip.GZIPOutputStream;

/**
 * Created by denismo on 4/10/15.
 */
public class S3Handler extends Handler {
    private AmazonS3Client client;
    private final Object writeLock = new Object();
    private int bufferSize;
    private String s3Bucket;
    private String s3Path;
    private ByteArrayOutputStream bos;
    private GZIPOutputStream stream;
    private ByteArrayOutputStream toBeStored;

    public S3Handler(AWSCredentialsProvider credentials, int bufferSize, String s3Bucket, String s3Path) {
        this.bufferSize = bufferSize;
        this.s3Bucket = s3Bucket;
        this.s3Path = s3Path;
        client = new AmazonS3Client(credentials);
        bos = new ByteArrayOutputStream();
        try {
            stream = new GZIPOutputStream(bos);
        } catch (IOException e) {
            e.printStackTrace();
            bos = null;
            stream = null;
        }
    }

    @Override
    public void publish(LogRecord record) {
        String formatted = formatMessage(record);
        synchronized (writeLock) {
            if (stream != null && bos != null) {
                try {
                    stream.write(formatted.getBytes("UTF-8"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (bos.size() > bufferSize && toBeStored == null) {
                    try {
                        stream.flush();
                        stream.finish();
                        stream.close();
                        toBeStored = bos;
                        bos = new ByteArrayOutputStream();
                        stream = new GZIPOutputStream(bos);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        // Discard in case of error
                        try {
                            bos = new ByteArrayOutputStream();
                            stream = new GZIPOutputStream(bos);
                        } catch (IOException e1) {
                            // Impossible
                        }
                    }
                }
            }
        }
        flush();
    }

    private String formatMessage(LogRecord record) {
        StringBuilder res = new StringBuilder();
        Instant inst = new Instant(record.getMillis());
        res.append("[").append(inst.toDateTime().toString("YYYY-MM-dd HH:mm:ss.SSS")).append("] ");
        res.append(record.getLevel()).append(": ");
        if (record.getParameters() == null || record.getParameters().length == 0) {
            res.append(record.getMessage()).append("\n");
        } else {
            try {
                res.append(MessageFormat.format(record.getMessage(), record.getParameters())).append("\n");
            } catch (Throwable e) {
                res.append(record.getMessage()).append("\n");
            }
        }

        try {
            if (record.getThrown() != null) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                pw.println();
                record.getThrown().printStackTrace(pw);
                pw.close();
                res.append(sw.toString());
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        return res.toString();
    }

    @Override
    public void flush() {
        ByteArrayOutputStream localBuffer;
        synchronized (writeLock) {
            if (toBeStored == null) return;
            localBuffer = toBeStored;
        }
        try {
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(localBuffer.size());
            meta.setContentType("text/plain; chartset=utf-8");
            DateTime date = DateTime.now();
            client.putObject(s3Bucket, s3Path + date.toString("/YYYY/MM/dd/HH-mm-ss") + ".gz", new ByteArrayInputStream(localBuffer.toByteArray()), meta);
        } catch (Throwable t) {
            t.printStackTrace();
            // Do not use logging due to potential recursion
        }
        synchronized (writeLock) {
            toBeStored = null;
        }
    }

    @Override
    public void close() throws SecurityException {
        synchronized (writeLock) {
            try {
                stream.flush();
                stream.finish();
                stream.close();
                toBeStored = bos;
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        flush();
    }
}
