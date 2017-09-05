package net.berla.aws.cloudfront;

import com.amazonaws.services.cloudfront;
import com.amazonaws.services.cloudfront.model.CreateInvalidationRequest;
import com.amazonaws.services.cloudfront.model.Paths;
import com.amazonaws.services.cloudfront.model.InvalidationBatch;

import net.berla.aws.Status;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class CloudFrontInvalidator{
    public Status call() {
	    	AWSCredentials awsCredentials = new DefaultAWSCredentialsProviderChain().getCredentials();
	    	AmazonCloudFrontClient client = new AmazonCloudFrontClient(awsCredentials);
	
	    	Paths invalidation_paths = new Paths().withItems("*").withQuantity(1);
	    	InvalidationBatch invalidation_batch = new InvalidationBatch(invalidation_paths, "" + System.currentTimeMillis());
	    	CreateInvalidationRequest invalidation = new CreateInvalidationRequest("distributionID", invalidation_batch);
	    	CreateInvalidationResult ret = client.createInvalidation(invalidation);
	    	
    		return Status.SUCCESS;
    }
}