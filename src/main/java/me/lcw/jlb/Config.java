package me.lcw.jlb;

import java.net.InetSocketAddress;
import java.util.Set;

public class Config {
  
  public static class LoadBalancerConfig {
    public String name;
    public boolean enabled;
    public HostPort localAddress;
    public EndpointConfig config;
    public Set<HostPort>  remoteHosts;
  }
  
  public static class EndpointConfig {
    public final int checkInterval;
    public final int minPassTillHealthy;
    public final int failsTillUnhealthy;
    public final int connectTimeout;
    
    EndpointConfig(int checkInterval, int connectTimeout, int minPassTillHealthy, int failsTillUnhealthy) {
      this.checkInterval = checkInterval;
      this.connectTimeout = connectTimeout;
      this.minPassTillHealthy = minPassTillHealthy;
      this.failsTillUnhealthy = failsTillUnhealthy;
    }
  }
  
  public static class HostPort {
    public final String host;
    public final int port;
    private transient volatile InetSocketAddress address = null; 
    
    public HostPort(String host, int port) {
      this.host =host;
      this.port = port;
      getAddress();
    }
    
    public InetSocketAddress getAddress() {
      if(address == null) {
        address = new InetSocketAddress(host, port);
      }
      return address;
    }
    
    @Override
    public int hashCode() {
      return this.getAddress().hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
      return this.getAddress().equals(o);
    }
  }
}
