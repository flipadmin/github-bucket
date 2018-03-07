package net.berla.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.Base64;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;
import net.berla.aws.Status;
import net.berla.aws.cloudfront.CloudFrontInvalidator;
import net.berla.aws.git.Branch;
import net.berla.aws.git.SyncableRepository;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.FilenameUtils;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.eclipse.jgit.lib.FileMode.TYPE_FILE;
import static org.eclipse.jgit.lib.FileMode.TYPE_MASK;

/**
 * Wrapper for an S3-Git-Repository. Handles working tree files synchronization.
 *
 * @author Matthias Berla (matthias@berla.net)
 * @version $Revision$ $Date$
 */
public class RepositoryS3 implements SyncableRepository {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoryS3.class);
    private static final Detector TIKA_DETECTOR = TikaConfig.getDefaultConfig().getDetector();

    private final AmazonS3 s3;
    private final Bucket bucket;
    private final Repository repository;
    private final URIish uri;
    private final Branch branch;
    private final CloudFrontInvalidator cloudFrontInvalidator;

    public RepositoryS3(Bucket bucket, AmazonS3 s3, CloudFrontInvalidator cloudFrontInvalidator, Repository repository, Branch branch) {
        this.s3 = s3;
        this.bucket = bucket;
        this.repository = repository;
        this.branch = branch;
        this.uri = new URIish().setScheme("amazon-s3").setHost(bucket.getName()).setPath(Constants.DOT_GIT);
        this.cloudFrontInvalidator = cloudFrontInvalidator;
    }

    @Override
    public Status call() throws Exception {
        // Get S3 file list
        final List<S3ObjectSummary> files = getS3ObjectSummaries();
        Map<String, S3ObjectSummary> diffMap = prepareDiffMap(files);

        try (final TreeWalk walker = new TreeWalk(repository)) {
            walker.addTree(getRevTree());
            walker.setRecursive(false);

            // Walk all files
            while (walker.next()) {
                // Enter directories
                if (walker.isSubtree()) {
                    walker.enterSubtree();
                    continue;
                }
                // Only accept file types (no symlinks, no gitlinks) as they cannot be created in S3
                if ((walker.getFileMode().getBits() & TYPE_MASK) != TYPE_FILE) {
                    continue;
                }
                // Here we have a real file!
                if (walk(diffMap, walker.getObjectId(0), walker.getPathString())) {
                    LOG.info("Uploaded file: {}", walker.getPathString());
                }
            }
        }

        // Delete remaining objects, as they are not in the repo anymore
        for (String file : diffMap.keySet()) {
            LOG.info("Deleting file: {}", file);
            s3.deleteObject(bucket.getName(), file);
        }

        // invalidate cloud front edges
        cloudFrontInvalidator.call();

        return Status.SUCCESS;
    }

  private Map<String,S3ObjectSummary> prepareDiffMap(List<S3ObjectSummary> files) {
    HashMap<String, S3ObjectSummary> result = new HashMap<>();
    for (S3ObjectSummary file : files) {
      result.put(file.getKey(), file);
    }
    return result;
  }

  private List<S3ObjectSummary> getS3ObjectSummaries() {
        // Do not include .git repository
        // matches: .git, .git/test...
        final Pattern excludePattern = Pattern.compile(String.format("^(\\.ssh|%s)(\\/.+)*$", Pattern.quote(Constants.DOT_GIT)));

        // S3 limits the size of the response to 1000 entries. Batch the requests.
        ObjectListing listing = s3.listObjects(bucket.getName());
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        while (listing.isTruncated()) {
            listing = s3.listNextBatchOfObjects(listing);
            summaries.addAll(listing.getObjectSummaries());
        }

        return summaries.stream().filter(file -> !excludePattern.matcher(file.getKey()).matches()).collect(Collectors.toList());
    }

    private RevTree getRevTree() throws IOException {
        Ref ref = repository.exactRef(branch.getFullRef());
        RevCommit commit = new RevWalk(repository).parseCommit(ref.getObjectId());
        return commit.getTree();
    }

    private boolean walk(Map<String, S3ObjectSummary> diffMap, ObjectId file, String path) throws IOException {
        byte[] content;
        byte[] newHash;
        LOG.debug("Start processing file: {}", path);
        try (DigestInputStream is = new DigestInputStream(repository.open(file).openStream(), DigestUtils.getMd5Digest())) {
            // Get content
            content = IOUtils.toByteArray(is);
            // Get hash
            newHash = is.getMessageDigest().digest();
        }
        if (isUploadFile(diffMap, path, Hex.encodeHexString(newHash))) {
            LOG.info("Uploading file: {}", path);
            ObjectMetadata bucketMetadata = new ObjectMetadata();
            bucketMetadata.setContentMD5(Base64.encodeAsString(newHash));
            bucketMetadata.setContentLength(content.length);
            // Give Tika a few hints for the content detection
            Metadata tikaMetadata = new Metadata();
            tikaMetadata.set(Metadata.RESOURCE_NAME_KEY, FilenameUtils.getName(FilenameUtils.normalize(path)));
            // Fire!
            try (InputStream bis = TikaInputStream.get(content, tikaMetadata)) {
                bucketMetadata.setContentType(TIKA_DETECTOR.detect(bis, tikaMetadata).toString());
                PutObjectRequest req = new PutObjectRequest(bucket.getName(), path, bis, bucketMetadata);
                req.setCannedAcl(CannedAccessControlList.PublicRead);
                s3.putObject(req);
                return true;
            }
        }

        LOG.info("Skipping file (same checksum): {}", path);
        return false;
    }

    private boolean isUploadFile(Map<String, S3ObjectSummary> diffMap, String path, String hash) {
      S3ObjectSummary existingFile = diffMap.remove(path);

      if (existingFile == null) {
        return true;
      }

      // Upload if the hashes differ
      return StringUtils.isNullOrEmpty(hash) || !existingFile.getETag().equals(hash);
    }

    @Override
    public URIish getUri() {
        return uri;
    }
}
