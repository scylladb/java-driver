package com.datastax.driver.core.utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class SocketUtils {

  public static boolean isTcpPortAvailable(int port) {
    try {
      ServerSocket serverSocket = new ServerSocket();
      try {
        serverSocket.setReuseAddress(false);
        serverSocket.bind(new InetSocketAddress(port), 1);
        return true;
      } finally {
        serverSocket.close();
      }
    } catch (IOException ex) {
      return false;
    }
  }
}
