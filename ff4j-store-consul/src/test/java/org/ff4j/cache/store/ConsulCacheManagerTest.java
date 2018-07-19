package org.ff4j.cache.store;

import com.google.common.base.Optional;
import com.google.common.io.BaseEncoding;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.async.ConsulResponseCallback;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.kv.ImmutableValue;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.option.QueryOptions;
import org.ff4j.TestDataSet;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigInteger;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ConsulCacheManagerTest {
  private static ConsulCacheManager cut;
  private static Consul mockConsul;

  private static KeyValueClient mockKeyValueClient;

  private boolean answeredTheCallback = false;
  private boolean failedTheCallback = false;

  /**
   * ConsulCacheManagerTest tests the FF4j ConsulCacheManager.
   */
  @BeforeClass
  public static void setUp() {
    mockConsul = mock(Consul.class);
    mockKeyValueClient = mock(KeyValueClient.class);
    cut = new ConsulCacheManager(mockConsul, "BN");
  }


  @Test
  public void testGetCacheProviderName() throws Exception {
    assertEquals(cut.getCacheProviderName(), "Consul");
  }

  @Test
  public void testCallbackAndCache() throws Exception {
    mockTheConsulKeyValueClientV0140();
    mockTheKeyValueClientGetValueWithAValueToTestCallbackV0140();
    cut.putFeature(TestDataSet.getFeatureEnabled());
    Set<String> featuresList = cut.listCachedFeatureNames();
    assertTrue(featuresList.contains(TestDataSet.getFeatureKeyName()));
  }


  @Test
  public void testCallbackWithFailureFromConsul() throws Exception {

    mockTheConsulKeyValueClientV0140();
    mockTheKeyValueClientGetValueWithFailureV0140();
    mockTheKeyValueClientGetValueWithAValueToTestCallbackV0140();
    cut.putFeature(TestDataSet.getFeatureEnabled());
    assertTrue(cut.getFeature(TestDataSet.getFeatureKeyName()).isEnable());
  }


  private void mockTheConsulKeyValueClientV0140() {
    doAnswer(client -> mockKeyValueClient).when(mockConsul).keyValueClient();
  }

  private void mockTheKeyValueClientGetValueWithAValueToTestCallbackV0140() throws Exception {

    doAnswer(
        new Answer<Void>() {

          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            //Since mocked version getvalue is called multiple times through the chain,
            //we don't want to simulate the call back from consul each time so flag it only once.
            if (!answeredTheCallback) {
              ConsulKeyValueChangeCallback watcher = invocation.getArgument(2);
              Optional<Value> value = Optional.of(ImmutableValue.builder()
                      .key(TestDataSet.getFullFeatureKeyNameCustom())
                      .value(BaseEncoding.base64().encode(TestDataSet
                      .getFeatureJsonEnabled()
                      .getBytes()))
                      .createIndex(449)
                      .lockIndex(2044)
                      .modifyIndex(0)
                      .flags(0)
                      .build());
              ConsulResponse<Optional<Value>> response = new ConsulResponse<>(value,
                      0,
                      true,
                      BigInteger.ONE);
              answeredTheCallback = true;
              watcher.onComplete(response);


            }
            return null;
          }
      }
    ).when(mockKeyValueClient)
            .getValue(ArgumentMatchers.any(String.class),
                    ArgumentMatchers.any(QueryOptions.class),
                    ArgumentMatchers.any(ConsulResponseCallback.class));
  }

  private void mockTheKeyValueClientGetValueWithFailureV0140() throws Exception {


    doAnswer(
        new Answer<Void>() {

          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            //Since mocked version getvalue is called multiple times through the chain,
            //we don't want to simulate the call back from consul each time so flag it only once.
            if (!failedTheCallback) {
              ConsulKeyValueChangeCallback watcher = invocation.getArgument(2);
              watcher.onFailure(new ConsulException("Test Failure"));
            }

            return null;
          }
        }
      ).when(mockKeyValueClient)
              .getValue(ArgumentMatchers.any(String.class),
                      ArgumentMatchers.any(QueryOptions.class),
                      ArgumentMatchers.any(ConsulResponseCallback.class));
  }
}