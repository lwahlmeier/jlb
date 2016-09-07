package me.lcw.jlb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;

import org.junit.Test;

public class IPPortTests {

  @Test
  public void simpleTest() {
    IPPort ipp1 = new IPPort("127.0.0.1", 6060);
    IPPort ipp2 = new IPPort(ipp1.getIPAsInt(), ipp1.getPort());
    assertEquals(ipp1, ipp2);
    assertEquals(ipp1.toString(), ipp2.toString());
    assertEquals(ipp1.hashCode(), ipp2.hashCode());
    assertEquals(new InetSocketAddress("127.0.0.1", 6060), ipp1.getIntSocketAddress());
    assertFalse(ipp1.equals( new IPPort("127.0.0.1", 6061)));
  }
  
  @Test
  public void simpleTest2() {
    IPPort ipp1 = new IPPort(758926109, 44060);
    IPPort ipp2 = new IPPort(ipp1.getIPAsString(), ipp1.getPort());
    assertEquals(ipp1, ipp2);
    assertEquals(ipp1.getIPAsString(), "45.60.75.29");
  }
  
  
  @Test
  public void badIP1() {
    try {
      new IPPort("445.12.12.12", 44060);
      fail();
    } catch(IllegalArgumentException e) {

    }
  }
  
  @Test
  public void badIP2() {
    try {
      new IPPort("44512.12.12", 44060);
      fail();
    } catch(IllegalArgumentException e) {

    }
  }
  
  
}
