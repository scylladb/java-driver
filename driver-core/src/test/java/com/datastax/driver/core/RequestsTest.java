package com.datastax.driver.core;

import static org.testng.Assert.assertEquals;

import com.datastax.driver.core.utils.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RequestsTest {

  public static class ExecuteTest {

    @Test(groups = "unit", dataProvider = "shouldProperlyEncodeMessages")
    void should_properly_encode_messages(
        MD5Digest resultMetadataId,
        ProtocolVersion protocolVersion,
        ProtocolFeatureStore protocolFeatureStore,
        int expectedSize) {
      // given
      ByteBuf destination = ByteBufAllocator.DEFAULT.buffer();
      Requests.Execute request =
          new Requests.Execute(
              MD5Digest.wrap(Bytes.fromHexString("0xcafebabe").array()),
              resultMetadataId,
              Requests.QueryProtocolOptions.DEFAULT,
              false);

      // when
      int size = Requests.Execute.coder.encodedSize(request, protocolVersion, protocolFeatureStore);
      Requests.Execute.coder.encode(request, destination, protocolVersion, protocolFeatureStore);

      // then
      assertEquals(size, expectedSize);
      assertEquals(destination.writerIndex(), expectedSize);
    }

    @DataProvider
    Object[][] shouldProperlyEncodeMessages() {
      return new Object[][] {
        {
          MD5Digest.wrap(Bytes.fromHexString("0xdeadbeef").array()),
          ProtocolVersion.V4,
          new ProtocolFeatureStore(null, null, null, false),
          9
        },
        {
          MD5Digest.wrap(Bytes.fromHexString("0xdeadbeef").array()),
          ProtocolVersion.V4,
          new ProtocolFeatureStore(null, null, null, true),
          15
        },
        {null, ProtocolVersion.V4, new ProtocolFeatureStore(null, null, null, true), 11},
        {
          MD5Digest.wrap(Bytes.fromHexString("0xdeadbeef").array()),
          ProtocolVersion.V5,
          new ProtocolFeatureStore(null, null, null, false),
          18
        },
        {null, ProtocolVersion.V5, new ProtocolFeatureStore(null, null, null, false), 14},
      };
    }
  }
}
