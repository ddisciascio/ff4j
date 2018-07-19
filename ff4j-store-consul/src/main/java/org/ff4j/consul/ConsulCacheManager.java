package org.ff4j.consul;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.orbitz.consul.Consul;
import org.ff4j.cache.InMemoryCacheEntry;
import org.ff4j.cache.InMemoryCacheManager;
import org.ff4j.core.Feature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


//Wraps the in memory cache manager so that cache can be updated from watches in Consul.

//This class will be contributed back to FF4J to allow community to be able to
//take advantage of updating the cache from consul's watch/callback capabilities.

/**The ConsulCacheManager class is a wrapper around FF4J's InMemoryCacheManager.
 * This class prevents FF4j from frequently accessing the consul running instance
 * by caching the values.  Additionally it has capabilities to listen to the consul
 * watch method to be able to get updates from outside of FF4j and update the cache
 * as necessary.
 *
 */
public final class ConsulCacheManager extends InMemoryCacheManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsulCacheManager.class);
  private Consul consul;
  private ConsulConstantsWrapper constants;
  private ObjectMapper mapper = new ObjectMapper();

  /** Creates an instance of ConsulCacheManager which has the ability to refresh the
   * cache from consul callbacks.
   *
   * @param consul - the instance of consul.
   * @param root - the root key name of all keys stored.
   */
  public ConsulCacheManager(Consul consul, String root) {
    super();
    Preconditions.checkNotNull(consul);
    this.consul = consul;
    constants = new ConsulConstantsWrapper(root);

  }

  @Override
  public String getCacheProviderName() {
    return "Consul";
  }

  private void watchForUpdates(Feature feature) {

    String key = constants.getPrefixFeatures() + feature.getUid();

    new ConsulKeyValueChangeCallback(consul, key, this::update, 100);
    LOGGER.info("Watching for updates on feature flag {}", feature.getUid());
  }

  @Override
  public void putFeature(Feature feature) {
    super.putFeature(feature);
    watchForUpdates(feature);

  }

  private void update(String json) {
    Feature feature;

    try {
      feature = fromJson(json);

      LOGGER.info("Feature flag with key {} was updated to {}", feature.getUid(),
              feature.isEnable());
      getFeaturesCache().replace(feature.getUid(), new InMemoryCacheEntry<>(feature));


    } catch (IOException e) {
      LOGGER.error("Failed to deserialize to a feature flag."
              + "  Updates to the cache will not take place."
              + "  Json was: {} ", json);

    }


  }

  //This needs to be moved to Feature class in FF4j.  It's not implemented today
  // and returns null if you try to use Feature.fromJson(...) to create a feature.
  // Implementing here as changing that class will require getting it approved
  // and merged into the FF4J project.  This should be done at a later date,
  // updated with the new version of the library and removed from here.

  private Feature fromJson(String json) throws IOException {
    if (json == null || json.isEmpty()) {
      throw new IllegalArgumentException("Json cannot be empty or null.");
    }

    Map<String, Object> map;

    // convert JSON string to Map
    map = mapper.readValue(json, new TypeReference<Map<String, Object>>() {
    });
    return new Feature(map.get("uid").toString(),
            (Boolean) map.get("enable"),
            map.get("description").toString());
  }

}