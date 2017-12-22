package me.lcw.jlb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.litesockets.ThreadedSocketExecuter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import me.lcw.jlb.Config.EndpointConfig;
import me.lcw.jlb.Config.HostPort;
import me.lcw.jlb.Config.RemoteHostPort;
import me.lcw.jlb.Config.TCPLoadBalancerConfig;

public class Main {
  public static final Gson GSON = new Gson();
  public static final Gson PGSON= new GsonBuilder().setPrettyPrinting().create();
  public static final String CONFIG_EXAMPLE;
  
  static {
    Config config = new Config();
    TCPLoadBalancerConfig lbc1 = new TCPLoadBalancerConfig();
    lbc1.name = "lb1";
    lbc1.localAddress = new HostPort("0.0.0.0", 8080);
    lbc1.config = new EndpointConfig(5000, 3000, 3, 2);
    lbc1.remoteHosts = new HashSet<RemoteHostPort>();
    lbc1.remoteHosts.add(new RemoteHostPort("192.168.2.50", 80, true));
    lbc1.remoteHosts.add(new RemoteHostPort("192.168.2.51", 80, false));
    
    TCPLoadBalancerConfig lbc2 = new TCPLoadBalancerConfig();
    lbc2.name = "lb2";
    lbc2.localAddress = new HostPort("0.0.0.0", 8443);
    lbc2.config = new EndpointConfig(5000, 1000, 3, 2);
    lbc2.remoteHosts = new HashSet<RemoteHostPort>();
    lbc2.remoteHosts.add(new RemoteHostPort("192.168.2.50", 443, false));
    lbc2.remoteHosts.add(new RemoteHostPort("192.168.2.51", 443, true));
    config.threads = 5;
    config.tcpLoadBalancers = new HashSet<>();
    config.tcpLoadBalancers.add(lbc1);
    config.tcpLoadBalancers.add(lbc2);
    CONFIG_EXAMPLE = PGSON.toJson(config);
  }
  
  public static final ConcurrentHashMap<String, TCPLoadBalancer> lbs = new ConcurrentHashMap<String, TCPLoadBalancer>();
  
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
    if(args.length == 0) {
      printHelp();
      System.exit(0);      
    }
    PriorityScheduler PS = new PriorityScheduler(Runtime.getRuntime().availableProcessors()*3);
    ThreadedSocketExecuter TSE = new ThreadedSocketExecuter(PS);
    TSE.start();
    File configFile = new File(args[0]);
    Config currentConfig = readConfigFile(configFile);
    long lastMod = 0;
    while(true) {
      if(lastMod != configFile.lastModified()) {
        currentConfig = readConfigFile(configFile);
        PS.setPoolSize(currentConfig.threads);
        for(TCPLoadBalancerConfig lbc: currentConfig.tcpLoadBalancers) {
          TCPLoadBalancer tcpLB;
          if(lbs.containsKey(lbc.name)) {
            tcpLB = lbs.get(lbc.name);
          } else {
            tcpLB = new TCPLoadBalancer(lbc.name, TSE, lbc.localAddress.getIPPort());
            tcpLB.start();
            lbs.put(lbc.name, tcpLB);
          }
          
          for(RemoteHostPort hp: lbc.remoteHosts) {
            tcpLB.addRemoteHost(hp);
          }
          Set<RemoteHostPort> origHosts = tcpLB.getRemoteHostPorts();
          origHosts.removeAll(lbc.remoteHosts); 
          for(RemoteHostPort rhp: origHosts) {
            tcpLB.removeRemoteHost(rhp);
          }
          tcpLB.setConfig(lbc.config);
        }
      }
      Thread.sleep(10000);
    }
  }
  
  public static Config readConfigFile(File cf) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(cf, "r");
    byte[] data = new byte[(int)raf.length()];
    raf.read(data);
    raf.close();
    return GSON.fromJson(new String(data), Config.class);
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
