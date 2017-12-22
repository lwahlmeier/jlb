package me.lcw.jlb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threadly.litesockets.Client;
import org.threadly.litesockets.SocketExecuter;
import org.threadly.litesockets.TCPClient;
import org.threadly.litesockets.TCPServer;

import me.lcw.jlb.Config.EndpointConfig;
import me.lcw.jlb.Config.RemoteHostPort;

public class TCPLoadBalancer {
  private static final Logger log = LoggerFactory.getLogger(TCPLoadBalancer.class);
  
  private final ConcurrentHashMap<IPPort, TCPEndPoint> endpoints = new ConcurrentHashMap<IPPort, TCPEndPoint>();
  private final SocketExecuter se;
  private final TCPServer server;
  private final IPPort localIPP;
  private final String id;
  
  private volatile EndpointConfig epc = new EndpointConfig(2000, 5000, 3, 1);
  
  
  public TCPLoadBalancer(String id, SocketExecuter se, IPPort fromIPP) throws IOException {
    this.id = id;
    this.se = se;
    this.localIPP = fromIPP;
    server = this.se.createTCPServer(localIPP.getIPAsString(), localIPP.getPort());
    server.setClientAcceptor((client)->onClientAccept(client));
  }
  
  public void start() {
    server.start();
    log.info("{}: started", this);
  }
  
  public void stop() {
    server.stop();
    log.info("{}: stopped", this);
  }
  
  public void setConfig(EndpointConfig epc) {
    this.epc = epc;
    for(TCPEndPoint ep: endpoints.values()) {
      ep.setConfig(epc);
    }
  }
  
  public boolean hasHealthyEndpoint() {
    for(TCPEndPoint tep: endpoints.values()) {
      if(tep.isEnabled() && tep.isHealthy()) {
        return true;
      }
    }
    return false;
  }
  
  public void addRemoteHost(RemoteHostPort ipp) {
    TCPEndPoint tep = new TCPEndPoint(id, se, ipp.getIPPort(), epc);
    TCPEndPoint ntep  = endpoints.putIfAbsent(ipp.getIPPort(), tep);
    if(ntep == null) {
      log.info("{}: new Endpoint added:{}",id, tep);
      ntep = tep;
    }
    if(ipp.enabled) {
      tep.enable();
    } else {
      tep.disable();
    }
  }
  
  public void removeRemoteHost(RemoteHostPort ipp) {
    TCPEndPoint tep = endpoints.remove(ipp.getIPPort());
    if(tep != null) {
      tep.disable();
      log.info("Endpoint removed:{}",tep);      
    }
  }
     
  public Set<RemoteHostPort> getRemoteHostPorts() {
    HashSet<RemoteHostPort> ltep = new HashSet<RemoteHostPort>();
    for(TCPEndPoint tep: endpoints.values()) {
      ltep.add(new RemoteHostPort(tep.getIPPort(), tep.isEnabled()));
    }
    return ltep;
  }
  
  protected Collection<TCPEndPoint> getEndPoints() {
    return endpoints.values();
  }
    
  protected TCPEndPoint getNextEndpoint() {
    int lastSize = 0;
    TCPEndPoint nextEP = null;
    List<TCPEndPoint> eps = new ArrayList<>(endpoints.values());
    Collections.shuffle(eps);
    for(TCPEndPoint tep: eps) {
      if(tep.isEnabled() && tep.isHealthy() && (lastSize == 0 || tep.clientSize() < lastSize)) {
        nextEP = tep;
      }
    }
    return nextEP;
  }

  private void onClientAccept(final Client client) {
    final TCPEndPoint tep = getNextEndpoint();
    final TCPClient tclient = (TCPClient)client;
    if(tep == null) {
      client.close();
      log.error("{}: could not add client to any endpoints!", id);
      return;
    }
    try {
      tep.addClient(tclient);
      return;
    } catch (IOException e) {
      client.close();
      log.error("{}: Problem adding client to {} error:{}", id, tep.toString(), e.getMessage());
    }
  }
  
  @Override
  public String toString() {
    return "LB:"+id+":"+this.localIPP;
  }
  
  
}
