package me.lcw.jlb;

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
    private transient volatile IPPort address = null; 
    
    public HostPort(String host, int port) {
      this.host =host;
      this.port = port;
      getIPPort();
    }
    
    public IPPort getIPPort() {
      if(address == null) {
        address = new IPPort(host, port);
      }
      return address;
    }
    
    @Override
    public int hashCode() {
      return this.getIPPort().hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
      return this.getIPPort().equals(o);
    }
  }
}
