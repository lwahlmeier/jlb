package me.lcw.jlb;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threadly.concurrent.future.FutureCallback;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.litesockets.Client;
import org.threadly.litesockets.Client.CloseListener;
import org.threadly.litesockets.Client.Reader;
import org.threadly.litesockets.SocketExecuter;
import org.threadly.litesockets.TCPClient;
import org.threadly.litesockets.utils.MergedByteBuffers;
import org.threadly.util.Clock;

import me.lcw.jlb.Config.EndpointConfig;

public class TCPEndPoint {
  private static final Logger log = LoggerFactory.getLogger(TCPEndPoint.class);
  private static final int MAX_WRITEBUFFER_SIZE = 64*1024;
  
  private final ConcurrentHashMap<TCPClient, TCPClient> clients = new ConcurrentHashMap<TCPClient, TCPClient>();
  private final ConcurrentLinkedQueue<Boolean> passFails = new ConcurrentLinkedQueue<Boolean>();
  private final ConnectionCheckRunnable ccr = new ConnectionCheckRunnable();
  private final SocketExecuter se;
  private final IPPort endpointISA;
  private final String id;

  private volatile EndpointConfig epc;
  private volatile boolean enabled = false;
  private volatile long lastAdd = Clock.lastKnownForwardProgressingMillis();
  
  public TCPEndPoint(String id, SocketExecuter se, IPPort endpoint, EndpointConfig epc) {
    this.id = id;
    this.se = se;
    this.endpointISA = endpoint;
    this.epc = epc;
  }
  
  public IPPort getIPPort() {
    return this.endpointISA;
  }
  
  public int clientSize() {
    return this.clients.size();
  }
  
  public long getLastTime() {
    return lastAdd;
  }
  
  public void addClient(final TCPClient inClient) throws IOException {
    if(!clients.containsKey(inClient)) {
      final TCPClient outClient = se.createTCPClient(endpointISA.getIPAsString(), endpointISA.getPort());
      outClient.clientOptions().setNativeBuffers(true);
      outClient.clientOptions().setReducedReadAllocations(true);
      if(clients.putIfAbsent(inClient, outClient) == null) {
        lastAdd = Clock.lastKnownForwardProgressingMillis();
        outClient.setReader(new ClientProxyReader(inClient));
        inClient.setReader(new ClientProxyReader(outClient));
        outClient.addCloseListener(new ClientProxyCloser(inClient));
        inClient.addCloseListener(new ClientProxyCloser(inClient));
        outClient.connect().addCallback(new FutureCallback<Boolean>() {
          @Override
          public void handleResult(Boolean result) {}

          @Override
          public void handleFailure(Throwable t) {
            outClient.close();
            inClient.close();
            clients.remove(inClient);
          }});
      }
    }
  }
  
  public void enable() {
    this.enabled = true;
    se.getThreadScheduler().execute(ccr);
    log.info("{}:{}: is Enabled", id, this.endpointISA);
  }
  
  public void disable() {
    this.enabled = false;
    log.info("{}:{}: is Disabled", id, this.endpointISA);
  }
  
  public boolean isEnabled() {
    return this.enabled;
  }
  
  public boolean isHealthy() {
    int fail = 0;
    int ccp = 0;
    for(Boolean b: passFails) {
      if(b) {
        ccp++;
      } else {
        fail++;
        ccp=0;
      }
    }
    if(ccp >= epc.minPassTillHealthy && fail < epc.failsTillUnhealthy) {
      return true;
    }
    return false;
  }
  
  public void setConfig(EndpointConfig epc) {
    this.epc = epc;
  }
  
  private class ConnectionCheckRunnable implements Runnable {
    private final AtomicBoolean checkRunning = new AtomicBoolean(false);

    @Override
    public void run() {
      if(enabled && checkRunning.compareAndSet(false, true)) {
        try {
          while(passFails.size() > (epc.failsTillUnhealthy + epc.minPassTillHealthy)) {
            passFails.poll();
          }
          final TCPClient c = se.createTCPClient(endpointISA.getIPAsString(), endpointISA.getPort());
          c.setConnectionTimeout(epc.connectTimeout);
          c.connect().addCallback(new FutureCallback<Boolean>() {

            @Override
            public void handleResult(Boolean result) {
              c.close();
              passFails.add(true);
              if(checkRunning.compareAndSet(true, false) && enabled) {
                se.getThreadScheduler().schedule(ConnectionCheckRunnable.this, epc.checkInterval);
              }
            }

            @Override
            public void handleFailure(Throwable t) {
              passFails.add(false);
              log.error("{}:{}, Check failed with error:{}, isHealthy:{}", id, endpointISA, t.getMessage(), isHealthy());
              if(checkRunning.compareAndSet(true, false) && enabled) {
                se.getThreadScheduler().schedule(ConnectionCheckRunnable.this, epc.checkInterval);
              }
            }});
        } catch(IOException e) {
          passFails.add(false);
          log.error("{}:{}, Check failed for with error:{}, isHealthy:{}", id, endpointISA, e.getMessage(), isHealthy());
          if(checkRunning.compareAndSet(true, false) && enabled) {
            se.getThreadScheduler().schedule(ConnectionCheckRunnable.this, epc.checkInterval);
          }
        }
      }
    }
  }
  
  @Override
  public String toString() {
    return "TCPEndPoint:"+id+":"+this.endpointISA;
  }
  
  private class ClientProxyCloser implements CloseListener {
    private final TCPClient sourceClient;
    
    private ClientProxyCloser(TCPClient sourceClient) {
      this.sourceClient = sourceClient;
    }

    @Override
    public void onClose(Client client) {
      sourceClient.close();
      TCPClient x = clients.remove(sourceClient);
      if(x != null) {
        x.close();
      }
    }
    
  }
  
  private static class ClientProxyReader implements Reader {
    private final TCPClient toClient;
    private final MergedByteBuffers mbb = new MergedByteBuffers();
    private volatile ListenableFuture<?> lastFuture = FutureUtils.immediateResultFuture(true);
    
    ClientProxyReader(TCPClient toClient) {
      this.toClient = toClient;
    }
    
    @Override
    public void onRead(final Client lfromClient) {
      if(toClient.getWriteBufferSize() < MAX_WRITEBUFFER_SIZE) {
        mbb.add(lfromClient.getRead());
        lastFuture =  toClient.write(mbb.pull(mbb.remaining()));
      } else {
        lastFuture.addListener(new Runnable() {
          @Override
          public void run() {
            onRead(lfromClient);
          }}, lfromClient.getClientsThreadExecutor());
      }
    }
    
  }
}

