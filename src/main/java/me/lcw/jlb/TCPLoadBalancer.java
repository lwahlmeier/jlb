package me.lcw.jlb;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threadly.litesockets.Client;
import org.threadly.litesockets.Server.ClientAcceptor;
import org.threadly.litesockets.SocketExecuter;
import org.threadly.litesockets.TCPClient;
import org.threadly.litesockets.TCPServer;

import me.lcw.jlb.Config.EndpointConfig;

public class TCPLoadBalancer {
  private static final Logger log = LoggerFactory.getLogger(TCPLoadBalancer.class);
  
  private final ConcurrentHashMap<IPPort, TCPEndPoint> endpoints = new ConcurrentHashMap<IPPort, TCPEndPoint>();
  private final ServerClientAcceptor sca = new ServerClientAcceptor();
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
    server.setClientAcceptor(sca);
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
    for(TCPEndPoint ep: getEndpointOrderedList()) {
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
  
  public void addEndpoint(IPPort ipp) {
    TCPEndPoint tep = new TCPEndPoint(id, se, ipp, epc);
    if(endpoints.putIfAbsent(ipp, tep) == null) {
      tep.enable();
      log.info("new Endpoint added:{}",tep);      
    }
  }
  
  public void removeEndpoint(IPPort ipp) {
    TCPEndPoint tep = new TCPEndPoint(id, se, ipp, epc);
    if(endpoints.putIfAbsent(ipp, tep) == null) {
      tep.enable();
      log.info("Endpoint removed:{}",tep);      
    }
  }
    
  public List<TCPEndPoint> getEndpointOrderedList() {
    LinkedList<TCPEndPoint> ltep = new LinkedList<TCPEndPoint>();
    for(TCPEndPoint tep: endpoints.values()) {
      if(tep.isEnabled() && tep.isHealthy()) {
        if(ltep.size() == 0) {
          ltep.add(tep);
        } else {
          int pos = 0;
          for(TCPEndPoint subtep: ltep) {
            if(subtep.clientSize() < tep.clientSize()) {
              pos++;
            } else if(subtep.clientSize() == tep.clientSize() && subtep.getLastTime() < tep.getLastTime()) {
              pos++;
            }
          }
          ltep.add(pos, tep);
        }

      }
    }
    return ltep;
  }
  
  public Collection<TCPEndPoint> getAllEndpoints() {
    return this.endpoints.values();
  }
  
  @Override
  public String toString() {
    return "LB:"+id+":"+this.localIPP;
  }
  
  
  private class ServerClientAcceptor implements ClientAcceptor {

    @Override
    public void accept(Client client) {
      final List<TCPEndPoint> ltep = getEndpointOrderedList();
      final TCPClient tclient = (TCPClient)client;
      tclient.clientOptions().setNativeBuffers(true);
      tclient.clientOptions().setReducedReadAllocations(true);
      if(ltep.size() == 0) {
        client.close();
      } else {
        for(TCPEndPoint tep: ltep) {
          try {
            tep.addClient(tclient);
            return;
          } catch (IOException e) {
            log.error("Problem adding client to {} error:{}", tep.toString(), e.getMessage());
          }
        }
        log.error("could not add client to any endpoint!");
        client.close();
      }
    }
    
  }
}
