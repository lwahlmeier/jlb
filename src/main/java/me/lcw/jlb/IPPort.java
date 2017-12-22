package me.lcw.jlb;

import java.net.InetSocketAddress;

public class IPPort {

  private final int ipInt;
  private final String ipString;
  private final int port;
  private final InetSocketAddress isa;
  
  public IPPort(String ip, int port) {
    this.ipString = ip.trim().intern();
    this.port = port;
    this.ipInt = stringIPToInt(ipString);
    isa = new InetSocketAddress(ipString, port);
  }
  
  public IPPort(int ip, int port) {
    this.ipInt = ip;
    this.port = port;
    this.ipString = intIPToString(ipInt).intern();
    isa = new InetSocketAddress(ipString, port);
  }
  
  public String getIPAsString() {
    return ipString;
  }
  
  public int getIPAsInt() {
    return ipInt;
  }
  
  public int getPort() {
    return port;
  }
  
  public InetSocketAddress getIntSocketAddress() {
    return isa;
  }
  
  @Override
  public String toString() {
    return ipString+":"+port;
  }
  
  @Override
  public int hashCode() {
    return ipInt^port;
  }
  
  @Override
  public boolean equals(Object o) {
    if(o instanceof IPPort) {
      IPPort ipp = (IPPort)o;
      if(this.ipInt == ipp.ipInt && this.port == ipp.port) {
        return true;
      }
    }
    return false;
  }
  
  public static int stringIPToInt(String ip) {
    String[] ips = ip.split("\\.");
    if(ips.length != 4) {
      throw new IllegalArgumentException("Not a valid IPv4 address:"+ip);
    }
    byte[] parts = new byte[4];
    try {
      int a = Integer.parseInt(ips[0]);
      int b = Integer.parseInt(ips[1]);
      int c = Integer.parseInt(ips[2]);
      int d = Integer.parseInt(ips[3]);
      if(a>255 || b>255 || c>255 || d>255 ) {
        throw new IllegalArgumentException("Not a valid IPv4 address:"+ip);
      }
      parts[0] = (byte)a;
      parts[1] = (byte)b;
      parts[2] = (byte)c;
      parts[3] = (byte)d;

      return parts[0]<<24 | parts[1]<<16 | parts[2]<<8 | parts[3];
    } catch (Exception e) {
      throw new IllegalArgumentException("Not a valid IPv4 address:"+ip, e);
    }
  }
  
  public static String intIPToString(int ip) {
    return (ip>>24) +"."+(ip>>16&0xff)+"."+(ip>>8&0xff)+"."+(ip&0xff);
  }
}
