package me.lcw.jlb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.litesockets.ThreadedSocketExecuter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import me.lcw.jlb.Config.EndpointConfig;
import me.lcw.jlb.Config.HostPort;
import me.lcw.jlb.Config.LoadBalancerConfig;

public class Main {
  public static final Gson GSON = new Gson();
  public static final Gson PGSON= new GsonBuilder().setPrettyPrinting().create();
  public static final String CONFIG_EXAMPLE;
  
  static {
    ArrayList<LoadBalancerConfig> al = new ArrayList<LoadBalancerConfig>();
    LoadBalancerConfig lbc1 = new LoadBalancerConfig();
    lbc1.name = "lb1";
    lbc1.localAddress = new HostPort("0.0.0.0", 8080);
    lbc1.config = new EndpointConfig(5000, 3000, 3, 2);
    lbc1.remoteHosts = new HashSet<HostPort>();
    lbc1.remoteHosts.add(new HostPort("192.168.2.50", 80));
    lbc1.remoteHosts.add(new HostPort("192.168.2.51", 80));
    
    LoadBalancerConfig lbc2 = new LoadBalancerConfig();
    lbc2.name = "lb2";
    lbc2.localAddress = new HostPort("0.0.0.0", 8443);
    lbc2.config = new EndpointConfig(5000, 1000, 3, 2);
    lbc2.remoteHosts = new HashSet<HostPort>();
    lbc2.remoteHosts.add(new HostPort("192.168.2.50", 443));
    lbc2.remoteHosts.add(new HostPort("192.168.2.51", 443));
    
    al.add(lbc1);
    al.add(lbc2);
    CONFIG_EXAMPLE = PGSON.toJson(al);
  }
  
  public static ConcurrentHashMap<String, TCPLoadBalancer> lbs = new ConcurrentHashMap<String, TCPLoadBalancer>();
  
  public static void printHelp() {
    System.out.println("\n\tcmd help: print this help message\n\nExample:\n\tcmd /path/To/Jsoncfg\n\nConfigExample:\n\n"+CONFIG_EXAMPLE);
  }
  public static void main(String[] args) throws IOException, InterruptedException, NoSuchAlgorithmException {
    for(String s: args){
      if(s.toLowerCase().contains("help")) {
        printHelp();
        System.exit(0);
      }
    }
    PriorityScheduler PS = new PriorityScheduler(Runtime.getRuntime().availableProcessors()*3);
    ThreadedSocketExecuter TSE = new ThreadedSocketExecuter(PS);
    TSE.start();
    String lastHash = "";

    while(true) {
      RandomAccessFile raf = new RandomAccessFile(args[0], "r");
      byte[] data = new byte[(int)raf.length()];
      raf.read(data);
      raf.close();
      String currentHash = hashByteArray(data);
      if(!currentHash.equals(lastHash)) {
        lastHash = currentHash;
        String config = new String(data);
        LoadBalancerConfig[] configs = GSON.fromJson(config, LoadBalancerConfig[].class);
        for(LoadBalancerConfig lbc: configs) {
          TCPLoadBalancer tcpLB;
          if(lbs.containsKey(lbc.name)) {
            tcpLB = lbs.get(lbc.name);
          } else {
            tcpLB = new TCPLoadBalancer(lbc.name, TSE, lbc.localAddress.getAddress());
            tcpLB.start();
            lbs.put(lbc.name, tcpLB);
          }
          HashSet<TCPEndPoint> origHosts = new HashSet<TCPEndPoint>(tcpLB.getAllEndpoints());
          for(HostPort hp: lbc.remoteHosts) {
            tcpLB.addEndpoint(hp.getAddress());
            TCPEndPoint toRemove = null;
            for(TCPEndPoint tep: origHosts) {
              if(tep.getAddress().equals(hp.getAddress())) {
                toRemove = tep;
                break;
              }
            }
            if(toRemove != null) {
              origHosts.remove(toRemove);
            }
            tcpLB.setConfig(lbc.config);
          }
          for(TCPEndPoint tep: origHosts) {
            tep.disable();
            if(tep.clientSize() == 0) {
              tcpLB.removeEndpoint(tep.getAddress());
            }
          }
        }
      }

      Thread.sleep(10000);
    }
  }
  
  public static String hashByteArray(byte[] data) throws NoSuchAlgorithmException, IOException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(data);
    byte[] mdbytes = md.digest();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < mdbytes.length; i++) {
      sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
    }
    return sb.toString();
  }
}
