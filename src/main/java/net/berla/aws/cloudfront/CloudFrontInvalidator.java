package net.berla.aws.cloudfront;

import com.amazonaws.services.cloudfront.AmazonCloudFront;
import com.amazonaws.services.cloudfront.AmazonCloudFrontClientBuilder;
import com.amazonaws.services.cloudfront.model.CreateInvalidationRequest;
import com.amazonaws.services.cloudfront.model.CreateInvalidationResult;
import com.amazonaws.services.cloudfront.model.Paths;
import com.amazonaws.services.cloudfront.model.InvalidationBatch;

import com.amazonaws.util.StringUtils;
import net.berla.aws.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloudFrontInvalidator{
	private static final Logger LOG = LoggerFactory.getLogger(CloudFrontInvalidator.class);

	private final String distribution;
	private final AmazonCloudFront client;

	public CloudFrontInvalidator(String distribution){
		if (StringUtils.isNullOrEmpty(distribution)) {
			throw new IllegalArgumentException();
		}

		this.distribution = distribution;
		this.client = AmazonCloudFrontClientBuilder.defaultClient();
	}


    public Status call() {
		LOG.info("Uploading file: {}", this.distribution);

		Paths invalidation_paths = new Paths().withItems("*").withQuantity(1);
		InvalidationBatch invalidation_batch = new InvalidationBatch(
			invalidation_paths,
			"" + System.currentTimeMillis()
		);

		CreateInvalidationRequest invalidation = new CreateInvalidationRequest(
			this.distribution,
			invalidation_batch
		);

		CreateInvalidationResult ret = this.client.createInvalidation(invalidation);

		return Status.SUCCESS;
    }
}