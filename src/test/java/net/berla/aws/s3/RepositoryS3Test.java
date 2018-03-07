package net.berla.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringUtils;
import net.berla.aws.Status;
import net.berla.aws.cloudfront.CloudFrontInvalidator;
import net.berla.aws.git.Branch;
import org.apache.commons.codec.digest.DigestUtils;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.junit.TestRepository;
import org.eclipse.jgit.lib.Constants;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static net.berla.aws.Status.SUCCESS;
import static net.berla.aws.s3.RepositoryS3Test.PutRequestMatcher.putRequest;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@SuppressWarnings("Duplicates")
@RunWith(MockitoJUnitRunner.class)
public class RepositoryS3Test {

  private static final Bucket BUCKET = new Bucket("TestBucket");

  private final AmazonS3 amazonS3 = mock(AmazonS3.class);

  private final CloudFrontInvalidator cloudFrontInvalidator = mock(CloudFrontInvalidator.class);

  private final TestRepository<InMemoryRepository> repository;

  private final RepositoryS3 objectUnderTest;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  public RepositoryS3Test() throws IOException {
    repository = new TestRepository<>(new InMemoryRepository(new DfsRepositoryDescription()));
    objectUnderTest = new RepositoryS3(BUCKET, amazonS3, cloudFrontInvalidator, repository.getRepository(), new Branch(Constants.MASTER));
  }

  @Test
  public void shouldRetainDotFiles() throws Exception {
    // Given
    repository.branch(Constants.MASTER).commit().create();

    prepareMockS3(Arrays.asList(createS3Object(".git/test"), createS3Object(".ssh/key")));

    // When
    Status status = objectUnderTest.call();

    // Then
    assertThat(status, is(SUCCESS));
    verify(amazonS3, times(1)).listObjects(eq(BUCKET.getName()));
    verify(amazonS3, times(0)).putObject(eq(BUCKET.getName()), any(), any(), any());
    verify(amazonS3, times(0)).deleteObject(eq(BUCKET.getName()), any());
    verifyNoMoreInteractions(amazonS3);
  }

  @Test
  public void shouldSync() throws Exception {
    // Given
    String pathFileA = "A";
    String pathFileB = "B";
    String pathFileC = "C";
    String pathFileD = "D";

    repository.branch(Constants.MASTER).commit()
        .add(pathFileC, repository.blob(generateContent(pathFileC)))
        .add(pathFileB, repository.blob(generateContent(pathFileB)))
        .add(pathFileD, repository.blob(generateContent(pathFileD))).create();

    prepareMockS3(Arrays.asList(createS3Object(pathFileA), createS3Object(pathFileC)));

    // When
    Status status = objectUnderTest.call();

    // Then
    assertThat(status, is(SUCCESS));
    verify(amazonS3, times(1)).listObjects(eq(BUCKET.getName()));
    verify(amazonS3, times(1)).putObject(argThat(putRequest(BUCKET.getName(), pathFileB)));
    verify(amazonS3, times(1)).putObject(argThat(putRequest(BUCKET.getName(), pathFileD)));
    verify(amazonS3, times(1)).deleteObject(eq(BUCKET.getName()), eq(pathFileA));
    verifyNoMoreInteractions(amazonS3);
  }

  @Test
  public void shouldDetectChanges() throws Exception {
    // Given
    String pathReadmeMd = "README.md";
    String oldContentReadmeMd = "This is a test file";
    String newContentReadmeMd = "This is a test file, with some changes";
    repository.branch(Constants.MASTER).commit().add(pathReadmeMd, repository.blob(newContentReadmeMd)).create();

    prepareMockS3(Collections.singletonList(createS3Object(pathReadmeMd, oldContentReadmeMd)));

    // When
    Status status = objectUnderTest.call();

    // Then
    assertThat(status, is(SUCCESS));
    verify(amazonS3, times(1)).listObjects(eq(BUCKET.getName()));
    verify(amazonS3, times(1)).putObject(argThat(putRequest(BUCKET.getName(), pathReadmeMd)));
    verify(amazonS3, times(0)).deleteObject(eq(BUCKET.getName()), any());
    verifyNoMoreInteractions(amazonS3);
  }

  private void prepareMockS3(List<S3ObjectSummary> expectedSummaries) {
    ObjectListing result = mock(ObjectListing.class);
    when(result.isTruncated()).thenReturn(false);
    when(result.getObjectSummaries()).thenReturn(expectedSummaries);
    when(amazonS3.listObjects(BUCKET.getName())).thenReturn(result);

    when(amazonS3.putObject(eq(BUCKET.getName()), any(), any(), any())).thenReturn(null);
    doNothing().when(amazonS3).deleteObject(eq(BUCKET.getName()), any());
  }

  private S3ObjectSummary createS3Object(String fileName, String content) {
    S3ObjectSummary summary;
    summary = new S3ObjectSummary();
    summary.setBucketName(BUCKET.getName());
    summary.setKey(fileName);
    summary.setETag(DigestUtils.md5Hex(content.getBytes(StringUtils.UTF8)));
    return summary;
  }

  private S3ObjectSummary createS3Object(String fileName) {
    return createS3Object(fileName, generateContent(fileName));
  }

  private String generateContent(String fileName) {
    return "This is a content of " + fileName;
  }


  public static class PutRequestMatcher extends TypeSafeDiagnosingMatcher<PutObjectRequest> {
    private final String bucketName;
    private final String key;

    private PutRequestMatcher(String bucketName, String key) {
      this.bucketName = bucketName;
      this.key = key;
    }

    @Override
    protected boolean matchesSafely(PutObjectRequest putObjectRequest, Description mismatchDescription) {
      if (!Objects.equals(putObjectRequest.getBucketName(), bucketName)) {
        mismatchDescription.appendText("a request with bucket: ")
            .appendValue(putObjectRequest.getBucketName());
        return false;
      }

      if (!Objects.equals(putObjectRequest.getKey(), key)) {
        mismatchDescription.appendText("a request with key: ")
            .appendValue(putObjectRequest.getKey());
        return false;
      }
      return true;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a request with bucket: ")
          .appendValue(bucketName)
          .appendText(" and key ")
          .appendValue(key);
    }

    public static PutRequestMatcher putRequest(String bucketName, String key) {
      return new PutRequestMatcher(bucketName, key);
    }
  }
}
