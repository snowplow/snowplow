package com.digdeep.util.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * Created by denismo on 4/10/15.
 */
public class EmptyAWSCredentialsProvider implements AWSCredentialsProvider {
    @Override
    public AWSCredentials getCredentials() {
        return null;
    }

    @Override
    public void refresh() {

    }
}
