/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

/** A listing of features that may or not apply to a given {@link ProtocolVersion}. */
public class ProtocolFeatures {

  /**
   * An abstract implementation of feature that may or not apply to a given {@link ProtocolVersion}.
   */
  public static class Feature {
    private final ProtocolVersion minSupportVersion;

    private Feature(ProtocolVersion minSupportVersion) {
      this.minSupportVersion = minSupportVersion;
    }

    /**
     * Determines whether the input version supports ths feature. Does not take optional features
     * from {@link ProtocolFeatureStore} into account.
     *
     * @param version the version to test against.
     * @return true if supported, false otherwise.
     * @see Feature#isSupportedBy(ProtocolVersion, ProtocolFeatureStore)
     */
    public boolean isSupportedBy(ProtocolVersion version) {
      return this.isSupportedBy(version, ProtocolFeatureStore.EMPTY);
    }

    /**
     * Determines whether the input version supports ths feature. Take optional features from {@link
     * ProtocolFeatureStore} into account if applicable.
     *
     * @param version the version to test against.
     * @param featureStore a feature store containing optional features.
     * @return rue if supported, false otherwise.
     */
    public boolean isSupportedBy(ProtocolVersion version, ProtocolFeatureStore featureStore) {
      return version.compareTo(minSupportVersion) >= 0;
    }
  }

  /**
   * The capability of updating a prepared statement if the result's metadata changes at runtime
   * (for example, if the query is a {@code SELECT *} and the table is altered).
   */
  public static final Feature PREPARED_METADATA_CHANGES =
      new Feature(ProtocolVersion.V5) {
        @Override
        public boolean isSupportedBy(ProtocolVersion version, ProtocolFeatureStore featureStore) {
          return super.isSupportedBy(version, featureStore)
              || (featureStore != null && featureStore.isUseMetadataId());
        }
      };

  /** The capability of sending or receiving custom payloads. */
  public static final Feature CUSTOM_PAYLOADS = new Feature(ProtocolVersion.V4);

  /** The capability of assigning client-generated timestamps to write requests. */
  public static final Feature CLIENT_TIMESTAMPS = new Feature(ProtocolVersion.V3);
}
