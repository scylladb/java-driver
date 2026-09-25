/*
 * Copyright (C) 2025 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.core;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLSession;

/**
 * A testable version of RemoteEndpointAwareNettySSLOptions that tracks SSL events for verification
 * in tests.
 */
public class TestableNettySSLOptions extends RemoteEndpointAwareNettySSLOptions {

  private static final boolean DEBUG = false;

  private final AtomicInteger handshakeCompletions = new AtomicInteger(0);
  private final AtomicInteger tls13Negotiations = new AtomicInteger(0);

  private final ConcurrentHashMap<String, SessionInfo> sessions = new ConcurrentHashMap<>();
  private final List<SessionInfo> sessionHistory = Collections.synchronizedList(new ArrayList<>());

  private final List<ClientHelloInfo> clientHelloHistory =
      Collections.synchronizedList(new ArrayList<>());

  public TestableNettySSLOptions(SslContext context) {
    super(context);
  }

  @Override
  public SslHandler newSSLHandler(SocketChannel channel, EndPoint remoteEndpoint) {
    SslHandler sslHandler = super.newSSLHandler(channel, remoteEndpoint);
    setupSslEventTracking(channel, sslHandler);
    return sslHandler;
  }

  private void setupSslEventTracking(SocketChannel channel, SslHandler sslHandler) {
    channel.pipeline().addFirst(new ClientHelloInspector());

    // Track handshake completion events
    sslHandler
        .handshakeFuture()
        .addListener(
            (GenericFutureListener<Future<? super io.netty.channel.Channel>>)
                future -> {
                  if (future.isSuccess()) {
                    handshakeCompletions.incrementAndGet();

                    SSLSession session = sslHandler.engine().getSession();
                    String protocol = session.getProtocol();
                    byte[] sessionId = session.getId();
                    String sessionIdHex = bytesToHex(sessionId);
                    long sessionCreationTime = session.getCreationTime();
                    long currentTime = System.currentTimeMillis();

                    if ("TLSv1.3".equals(protocol)) {
                      tls13Negotiations.incrementAndGet();
                    }

                    // Create session info
                    SessionInfo sessionInfo =
                        new SessionInfo(
                            sessionIdHex,
                            sessionCreationTime,
                            currentTime,
                            protocol,
                            session.getCipherSuite(),
                            channel.remoteAddress().toString());

                    if (!sessions.containsKey(sessionIdHex)) {
                      sessions.put(sessionIdHex, sessionInfo);
                    }

                    sessionHistory.add(sessionInfo);
                  }
                });
  }

  private String bytesToHex(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return "empty";
    }
    StringBuilder result = new StringBuilder();
    for (byte b : bytes) {
      result.append(String.format("%02x", b));
    }
    return result.toString();
  }

  public int getHandshakeCompletions() {
    return handshakeCompletions.get();
  }

  public int getTls13Negotiations() {
    return tls13Negotiations.get();
  }

  public int getUniqueSessionsCount() {
    return sessions.size();
  }

  public List<SessionInfo> getSessionHistory() {
    return new ArrayList<>(sessionHistory);
  }

  public List<ClientHelloInfo> getClientHelloHistory() {
    return new ArrayList<>(clientHelloHistory);
  }

  // Reset counters for test setup
  public void resetCounters() {
    handshakeCompletions.set(0);
    tls13Negotiations.set(0);
    sessions.clear();
    sessionHistory.clear();
    clientHelloHistory.clear();
  }

  // Print session information to standard output
  public void printSessionInfo() {
    System.out.println("=== SSL Session Information ===");
    System.out.println("Total handshakes: " + getHandshakeCompletions());
    System.out.println("TLS 1.3 negotiations: " + getTls13Negotiations());
    System.out.println("Unique sessions: " + getUniqueSessionsCount());
    System.out.println();

    System.out.println("=== Session History ===");
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    for (int i = 0; i < sessionHistory.size(); i++) {
      SessionInfo info = sessionHistory.get(i);
      System.out.println("Handshake #" + (i + 1) + ":");
      System.out.println("  Session ID: " + info.getSessionId());
      System.out.println("  Creation Time: " + dateFormat.format(new Date(info.getCreationTime())));
      System.out.println(
          "  Handshake Time: " + dateFormat.format(new Date(info.getHandshakeTime())));
      System.out.println("  Protocol: " + info.getProtocol());
      System.out.println("  Cipher Suite: " + info.getCipherSuite());
      System.out.println("  Remote Address: " + info.getRemoteAddress());
      System.out.println(
          "  Age at handshake: " + (info.getHandshakeTime() - info.getCreationTime()) + "ms");
      System.out.println();
    }

    System.out.println("=== Unique Sessions ===");
    for (SessionInfo info : sessions.values()) {
      System.out.println(
          "Session ID: "
              + info.getSessionId()
              + " | Created: "
              + dateFormat.format(new Date(info.getCreationTime()))
              + " | Protocol: "
              + info.getProtocol()
              + " | Cipher: "
              + info.getCipherSuite());
    }

    System.out.println("=== ClientHello History ===");
    for (ClientHelloInfo helloInfo : clientHelloHistory) {
      System.out.println(
          "ClientHello ID: "
              + helloInfo.getClientHelloId()
              + " | Created: "
              + dateFormat.format(new Date(helloInfo.getCreationTime()))
              + " | Session ID: "
              + helloInfo.getSessionId()
              + " | Has PSK Extension: "
              + helloInfo.hasPreSharedKeyExtension());

      if (helloInfo.hasPreSharedKeyExtension()) {
        System.out.println(
            "  Pre-shared Keys (" + helloInfo.getPreSharedKeys().size() + " total):");
        for (int i = 0; i < helloInfo.getPreSharedKeys().size(); i++) {
          PreSharedKeyInfo psk = helloInfo.getPreSharedKeys().get(i);
          System.out.println("    PSK[" + i + "] Identity: " + psk.getIdentity());
          System.out.println(
              "    PSK[" + i + "] Obfuscated Ticket Age: " + psk.getObfuscatedTicketAge());
        }
      }
    }
    System.out.println("==============================");
  }

  public static class SessionInfo {
    private final String sessionId;
    private final long creationTime;
    private final long handshakeTime;
    private final String protocol;
    private final String cipherSuite;
    private final String remoteAddress;

    public SessionInfo(
        String sessionId,
        long creationTime,
        long handshakeTime,
        String protocol,
        String cipherSuite,
        String remoteAddress) {
      this.sessionId = sessionId;
      this.creationTime = creationTime;
      this.handshakeTime = handshakeTime;
      this.protocol = protocol;
      this.cipherSuite = cipherSuite;
      this.remoteAddress = remoteAddress;
    }

    public String getSessionId() {
      return sessionId;
    }

    public long getCreationTime() {
      return creationTime;
    }

    public long getHandshakeTime() {
      return handshakeTime;
    }

    public String getProtocol() {
      return protocol;
    }

    public String getCipherSuite() {
      return cipherSuite;
    }

    public String getRemoteAddress() {
      return remoteAddress;
    }

    @Override
    public String toString() {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      return String.format(
          "SessionInfo{id=%s, created=%s, handshake=%s, protocol=%s, cipher=%s, remote=%s}",
          sessionId,
          dateFormat.format(new Date(creationTime)),
          dateFormat.format(new Date(handshakeTime)),
          protocol,
          cipherSuite,
          remoteAddress);
    }
  }

  // Inner class to hold new session ticket information
  public static class NewSessionTicketInfo {
    private final String ticketId;
    private final long creationTime;
    private final String sessionId;
    private boolean resumed;

    public NewSessionTicketInfo(String ticketId, long creationTime, String sessionId) {
      this.ticketId = ticketId;
      this.creationTime = creationTime;
      this.sessionId = sessionId;
      this.resumed = false;
    }

    public String getTicketId() {
      return ticketId;
    }

    public long getCreationTime() {
      return creationTime;
    }

    public String getSessionId() {
      return sessionId;
    }

    public boolean isResumed() {
      return resumed;
    }

    public void setResumed(boolean resumed) {
      this.resumed = resumed;
    }

    @Override
    public String toString() {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      return String.format(
          "NewSessionTicketInfo{ticketId=%s, created=%s, sessionId=%s, resumed=%s}",
          ticketId, dateFormat.format(new Date(creationTime)), sessionId, resumed);
    }
  }

  public static class ClientHelloInfo {
    private final String clientHelloId;
    private final long creationTime;
    private final String sessionId;
    private final List<PreSharedKeyInfo> preSharedKeys;
    private final boolean hasPreSharedKeyExtension;

    public ClientHelloInfo(String clientHelloId, long creationTime, String sessionId) {
      this.clientHelloId = clientHelloId;
      this.creationTime = creationTime;
      this.sessionId = sessionId;
      this.preSharedKeys = new ArrayList<>();
      this.hasPreSharedKeyExtension = false;
    }

    public ClientHelloInfo(
        String clientHelloId,
        long creationTime,
        String sessionId,
        List<PreSharedKeyInfo> preSharedKeys) {
      this.clientHelloId = clientHelloId;
      this.creationTime = creationTime;
      this.sessionId = sessionId;
      this.preSharedKeys =
          preSharedKeys != null ? new ArrayList<>(preSharedKeys) : new ArrayList<>();
      this.hasPreSharedKeyExtension = !this.preSharedKeys.isEmpty();
    }

    public String getClientHelloId() {
      return clientHelloId;
    }

    public long getCreationTime() {
      return creationTime;
    }

    public String getSessionId() {
      return sessionId;
    }

    public List<PreSharedKeyInfo> getPreSharedKeys() {
      return new ArrayList<>(preSharedKeys);
    }

    public boolean hasPreSharedKeyExtension() {
      return hasPreSharedKeyExtension;
    }

    @Override
    public String toString() {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      return String.format(
          "ClientHelloInfo{clientHelloId=%s, created=%s, sessionId=%s, hasPreSharedKeys=%s, preSharedKeyCount=%d}",
          clientHelloId,
          dateFormat.format(new Date(creationTime)),
          sessionId,
          hasPreSharedKeyExtension,
          preSharedKeys.size());
    }
  }

  // Inner class to hold pre-shared key information
  public static class PreSharedKeyInfo {
    private final String identity;
    private final int obfuscatedTicketAge;

    public PreSharedKeyInfo(String identity, int obfuscatedTicketAge) {
      this.identity = identity;
      this.obfuscatedTicketAge = obfuscatedTicketAge;
    }

    public String getIdentity() {
      return identity;
    }

    public int getObfuscatedTicketAge() {
      return obfuscatedTicketAge;
    }

    @Override
    public String toString() {
      return String.format(
          "PreSharedKeyInfo{identity=%s, obfuscatedAge=%d}", identity, obfuscatedTicketAge);
    }
  }

  private class ClientHelloInspector extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
      if (msg instanceof ByteBuf) {
        ByteBuf buffer = (ByteBuf) msg;

        // Check if this looks like a TLS handshake message
        if (buffer.readableBytes() >= 6) {
          // Make a copy to inspect without affecting the original buffer
          ByteBuf copy = buffer.duplicate();

          // TLS record header: type (1 byte) + version (2 bytes) + length (2 bytes)
          byte contentType = copy.readByte();
          short version = copy.readShort();
          short length = copy.readShort();

          // Check if this is a handshake record (content type 22)
          if (contentType == 22 && copy.readableBytes() >= 4) {
            // Handshake message header: type (1 byte) + length (3 bytes)
            byte handshakeType = copy.readByte();

            // Check if this is a ClientHello (handshake type 1)
            if (handshakeType == 1) {
              // Read the handshake message length (3 bytes, big-endian)
              int messageLength =
                  (copy.readByte() & 0xFF) << 16
                      | (copy.readByte() & 0xFF) << 8
                      | (copy.readByte() & 0xFF);

              if (copy.readableBytes() >= Math.min(messageLength, 34)) {
                ClientHelloInfo clientHelloInfo = parseClientHello(copy, messageLength);
                if (clientHelloInfo != null) {
                  clientHelloHistory.add(clientHelloInfo);

                  if (DEBUG) {
                    System.out.println("=== ClientHello Detected ===");
                    System.out.println("ClientHello ID: " + clientHelloInfo.getClientHelloId());
                    System.out.println("Session ID: " + clientHelloInfo.getSessionId());
                    System.out.println(
                        "Timestamp: "
                            + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
                                .format(new Date(clientHelloInfo.getCreationTime())));
                    System.out.println("Raw message length: " + messageLength + " bytes");
                    System.out.println(
                        "Has pre_shared_key extension: "
                            + clientHelloInfo.hasPreSharedKeyExtension());
                    if (clientHelloInfo.hasPreSharedKeyExtension()) {
                      System.out.println(
                          "Pre-shared keys count: " + clientHelloInfo.getPreSharedKeys().size());
                      for (int i = 0; i < clientHelloInfo.getPreSharedKeys().size(); i++) {
                        PreSharedKeyInfo psk = clientHelloInfo.getPreSharedKeys().get(i);
                        System.out.println("  PSK[" + i + "] Identity: " + psk.getIdentity());
                        System.out.println(
                            "  PSK["
                                + i
                                + "] Obfuscated Ticket Age: "
                                + psk.getObfuscatedTicketAge());
                      }
                    }
                    System.out.println("============================");
                  }
                }
              }
            }
          }
        }
      }

      // Pass the message along unchanged
      super.write(ctx, msg, promise);
    }

    private ClientHelloInfo parseClientHello(ByteBuf buffer, int messageLength) {
      try {
        // Skip protocol version (2 bytes)
        buffer.skipBytes(2);

        // Skip client random (32 bytes)
        buffer.skipBytes(32);

        // Read session ID length (1 byte)
        if (buffer.readableBytes() < 1) return null;
        int sessionIdLength = buffer.readUnsignedByte();

        // Read session ID
        String sessionId = "empty";
        if (sessionIdLength > 0 && buffer.readableBytes() >= sessionIdLength) {
          byte[] sessionIdBytes = new byte[sessionIdLength];
          buffer.readBytes(sessionIdBytes);
          sessionId = bytesToHex(sessionIdBytes);
        }

        // Skip cipher suites length (2 bytes) and cipher suites
        if (buffer.readableBytes() < 2) return null;
        int cipherSuitesLength = buffer.readUnsignedShort();
        if (buffer.readableBytes() < cipherSuitesLength) return null;
        buffer.skipBytes(cipherSuitesLength);

        // Skip compression methods length (1 byte) and compression methods
        if (buffer.readableBytes() < 1) return null;
        int compressionMethodsLength = buffer.readUnsignedByte();
        if (buffer.readableBytes() < compressionMethodsLength) return null;
        buffer.skipBytes(compressionMethodsLength);

        // Parse extensions if present
        List<PreSharedKeyInfo> preSharedKeys = new ArrayList<>();
        if (buffer.readableBytes() >= 2) {
          // Read extensions length (2 bytes)
          int extensionsLength = buffer.readUnsignedShort();
          int extensionsStart = buffer.readerIndex();
          int extensionsEnd = extensionsStart + extensionsLength;

          // Parse each extension
          while (buffer.readerIndex() < extensionsEnd && buffer.readableBytes() >= 4) {
            int extType = buffer.readUnsignedShort();
            int extLength = buffer.readUnsignedShort();

            if (buffer.readableBytes() < extLength) {
              // Not enough data for this extension
              break;
            }

            // Check for pre_shared_key extension (type 41)
            if (extType == 41) {
              preSharedKeys = parsePreSharedKeyExtension(buffer, extLength);
            } else {
              // Skip this extension
              buffer.skipBytes(extLength);
            }
          }
        }

        // Generate a unique ID for this ClientHello
        String clientHelloId =
            "ch_" + System.currentTimeMillis() + "_" + Math.abs(buffer.hashCode() % 1000);

        return new ClientHelloInfo(
            clientHelloId, System.currentTimeMillis(), sessionId, preSharedKeys);

      } catch (Exception e) {
        // If parsing fails, return null
        System.err.println("Failed to parse ClientHello: " + e.getMessage());
        return null;
      }
    }

    private List<PreSharedKeyInfo> parsePreSharedKeyExtension(ByteBuf buffer, int extLength) {
      List<PreSharedKeyInfo> preSharedKeys = new ArrayList<>();
      try {
        int startIndex = buffer.readerIndex();
        int endIndex = startIndex + extLength;

        // Read identities length (2 bytes)
        if (buffer.readableBytes() < 2) return preSharedKeys;
        int identitiesLength = buffer.readUnsignedShort();

        int identitiesStart = buffer.readerIndex();
        int identitiesEnd = identitiesStart + identitiesLength;

        // Parse each identity
        while (buffer.readerIndex() < identitiesEnd && buffer.readableBytes() >= 2) {
          // Read identity length (2 bytes)
          int identityLength = buffer.readUnsignedShort();

          if (buffer.readableBytes() < identityLength + 4) {
            // Not enough data for identity + obfuscated_ticket_age
            break;
          }

          // Read identity data
          byte[] identityBytes = new byte[identityLength];
          buffer.readBytes(identityBytes);
          String identity = bytesToHex(identityBytes);

          // Read obfuscated_ticket_age (4 bytes)
          int obfuscatedTicketAge = buffer.readInt();

          preSharedKeys.add(new PreSharedKeyInfo(identity, obfuscatedTicketAge));
        }

        // Skip any remaining bytes in the extension (like PSK binders)
        int remainingBytes = endIndex - buffer.readerIndex();
        if (remainingBytes > 0 && buffer.readableBytes() >= remainingBytes) {
          buffer.skipBytes(remainingBytes);
        }

      } catch (Exception e) {
        throw new RuntimeException("Failed to parse pre_shared_key extension", e);
      }
      return preSharedKeys;
    }
  }
}
