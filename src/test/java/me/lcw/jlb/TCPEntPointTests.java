package me.lcw.jlb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.litesockets.Client;
import org.threadly.litesockets.Client.ClientCloseListener;
import org.threadly.litesockets.Client.Reader;
import org.threadly.litesockets.Server.ClientAcceptor;
import org.threadly.litesockets.TCPClient;
import org.threadly.litesockets.TCPServer;
import org.threadly.litesockets.ThreadedSocketExecuter;
import org.threadly.litesockets.buffers.MergedByteBuffers;
import org.threadly.litesockets.utils.PortUtils;
import org.threadly.test.concurrent.TestCondition;

import me.lcw.jlb.Config.EndpointConfig;
import me.lcw.jlb.Config.RemoteHostPort;

public class TCPEntPointTests {
  
  
  @Test
  public void FailureTest() throws InterruptedException, IOException, ExecutionException {
    final int server_size = 1;
    final int client_size = 1;
    PriorityScheduler PS = new PriorityScheduler(5);
    ThreadedSocketExecuter TSE = new ThreadedSocketExecuter(PS);
    TSE.start();
    int lbPort = PortUtils.findTCPPort();
    final TCPLoadBalancer rlb = new TCPLoadBalancer("1", TSE, new IPPort("127.0.0.1", lbPort));
    rlb.setConfig(new EndpointConfig(100, 1000, 1, 1));
    
    
    final EchoServer[] servers = new EchoServer[server_size];
    RemoteHostPort[] server_isa = new RemoteHostPort[server_size];
    
    for(int i=0; i<server_size; i++) {
      int port = PortUtils.findTCPPort();
      servers[i] = new EchoServer(TSE.createTCPServer("127.0.0.1", port), port);
      server_isa[i] = new RemoteHostPort("127.0.0.1", port, true);
      rlb.addRemoteHost(server_isa[i]);
    }
    rlb.start();
    
    new TestCondition(){
      @Override
      public boolean get() {
        return rlb.hasHealthyEndpoint();
      }
    }.blockTillTrue(5000);

    final ArrayList<TCPClient> clients = new ArrayList<TCPClient>(); 
    for(int i=0; i<client_size; i++) {
      TCPClient c = TSE.createTCPClient("127.0.0.1", lbPort);
      c.connect().get();
      clients.add(c);
      c.write(ByteBuffer.wrap("TEST".getBytes()));
    }
    
    new TestCondition() {
      @Override
      public boolean get() {
        int size =0;
        for(EchoServer es: servers) {
          size+=es.clients.get();
        }
        return size == client_size;
      }
    }.blockTillTrue(5000);
    
    for(EchoServer es: servers) {
      es.server.close();
      break;
    }
    
    new TestCondition() {
      @Override
      public boolean get() {
        for(TCPEndPoint ep: rlb.getEndPoints()) {
          if(!ep.isHealthy()) {
            return true;
          }
        }
        return false;
      }
    }.blockTillTrue(5000, 100);

  }

  
  @Test
  public void SimpleLBTest() throws InterruptedException, IOException, ExecutionException {
    final int server_size = 5;
    final int client_size = 10;
    PriorityScheduler PS = new PriorityScheduler(5);
    ThreadedSocketExecuter TSE = new ThreadedSocketExecuter(PS);
    TSE.start();
    int lbPort = PortUtils.findTCPPort();
    final TCPLoadBalancer rlb = new TCPLoadBalancer("1", TSE, new IPPort("127.0.0.1", lbPort));
    rlb.setConfig(new EndpointConfig(100, 1000, 1, 1));
    
    
    final EchoServer[] servers = new EchoServer[server_size];
    RemoteHostPort[] server_isa = new RemoteHostPort[server_size];
    
    for(int i=0; i<server_size; i++) {
      int port = PortUtils.findTCPPort();
      servers[i] = new EchoServer(TSE.createTCPServer("127.0.0.1", port), port);
      server_isa[i] = new RemoteHostPort("127.0.0.1", port, true);
      rlb.addRemoteHost(server_isa[i]);
    }
    rlb.start();
    
    new TestCondition(){
      @Override
      public boolean get() {
        return rlb.hasHealthyEndpoint();
      }
    }.blockTillTrue(5000);

    final ArrayList<TCPClient> clients = new ArrayList<TCPClient>(); 
    for(int i=0; i<client_size; i++) {
      TCPClient c = TSE.createTCPClient("127.0.0.1", lbPort);
      c.connect().get();
      clients.add(c);
      c.write(ByteBuffer.wrap("TEST".getBytes()));
    }
    
    new TestCondition(){
      @Override
      public boolean get() {
        int size =0;
        for(EchoServer es: servers) {
          size+=es.clients.get();
        }
        return size == client_size;
      }
    }.blockTillTrue(5000);
    
    new TestCondition(){
      @Override
      public boolean get() {
        int size =0;
        for(TCPClient c: clients) {
          size+=c.getReadBufferSize();
        }
        return size == client_size*4;
      }
    }.blockTillTrue(5000);
    for(TCPClient c: clients) {
      c.close();
    }
    new TestCondition(){
      @Override
      public boolean get() {
        int size = 0;
        for(EchoServer es: servers) {
          size+=es.clients.get();
        }
        return size == 0;
      }
    }.blockTillTrue(5000);
  }
  
  
  @Test
  public void SimpleLBBlockingTest() throws InterruptedException, IOException, ExecutionException {
    final int server_size = 10;
    final int client_size = 10;
    ByteBuffer bb = ByteBuffer.wrap(new byte[1024*128]);
    PriorityScheduler PS = new PriorityScheduler(5);
    ThreadedSocketExecuter TSE = new ThreadedSocketExecuter(PS);
    TSE.start();
    int lbPort = PortUtils.findTCPPort();
    final TCPLoadBalancer rlb = new TCPLoadBalancer("1", TSE, new IPPort("127.0.0.1", lbPort));
    rlb.setConfig(new EndpointConfig(100, 1000, 1, 1));
    
    
    final EchoServer[] servers = new EchoServer[server_size];
    RemoteHostPort[] server_isa = new RemoteHostPort[server_size];
    
    for(int i=0; i<server_size; i++) {
      int port = PortUtils.findTCPPort();
      servers[i] = new EchoServer(TSE.createTCPServer("127.0.0.1", port), port);
      server_isa[i] = new RemoteHostPort("127.0.0.1", port, true);
      rlb.addRemoteHost(server_isa[i]);
      servers[i].doRead = false;
    }
    rlb.start();
    
    new TestCondition(){
      @Override
      public boolean get() {
        return rlb.hasHealthyEndpoint();
      }
    }.blockTillTrue(5000);

    final ArrayList<TCPClient> clients = new ArrayList<TCPClient>(); 
    for(int i=0; i<client_size; i++) {
      TCPClient c = TSE.createTCPClient("127.0.0.1", lbPort);
      c.setReader(new Reader() {

        @Override
        public void onRead(Client client) {
          client.getRead();
        }});
      c.connect().get();
      clients.add(c);
    }
    
    new TestCondition(){
      @Override
      public boolean get() {
        int size =0;
        for(EchoServer es: servers) {
          size+=es.clients.get();
        }
        return size == client_size;
      }
    }.blockTillTrue(5000);
    for(TCPClient c: clients) {
      c.write(bb.duplicate());
      c.write(bb.duplicate());
      c.write(bb.duplicate());
      c.write(bb.duplicate());
    }

    for(EchoServer es: servers) {
      es.doRead = true;
      es.forceClientRead();
    }
    new TestCondition(){
      @Override
      public boolean get() {
        int size =0;
        for(TCPClient c: clients) {
          size+=c.getStats().getTotalRead();
        }
        
        return size == client_size*(bb.remaining()*4);
      }
    }.blockTillTrue(5000);
  }
  
  public class EchoServer implements ClientAcceptor, Reader, ClientCloseListener {
    AtomicInteger clients = new AtomicInteger(0);
    AtomicInteger data = new AtomicInteger(0);
    CopyOnWriteArraySet<TCPClient> tcp_clients = new CopyOnWriteArraySet<TCPClient>(); 
    final TCPServer server;
    final int port;
    volatile boolean doEcho = true;
    volatile boolean doRead = true;
    
    public EchoServer(TCPServer server, int port) {
      this.port = port;
      this.server = server;
      this.server.setClientAcceptor(this);
      server.start();
    }

    @Override
    public void onClose(Client client) {
      clients.decrementAndGet();
    }

    @Override
    public void onRead(Client client) {
      if(doRead) {
        MergedByteBuffers mbb = client.getRead();
        data.addAndGet(mbb.remaining());
        if(doEcho) {
          try {
            client.write(mbb);
          } catch(Exception e) {
            
          }
        }
      }
    }
    
    public void forceClientRead() {
      for(TCPClient c: tcp_clients) {
        onRead(c);
      }
    }

    @Override
    public void accept(Client client) {
      tcp_clients.add((TCPClient)client);
      clients.incrementAndGet();
      client.setReader(this);
      client.addCloseListener(this);
    }
  }
}
