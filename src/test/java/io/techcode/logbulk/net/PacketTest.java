/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.techcode.logbulk.net;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for Packet.
 */
public class PacketTest {

    private static final String TEST_ROUTE = "foobar";

    @Test(expected = NullPointerException.class) public void testConstruct1() throws Exception {
        Packet.builder()
                .header(null)
                .body(null)
                .build();
    }

    @Test(expected = NullPointerException.class) public void testConstruct2() throws Exception {
        Packet.builder()
                .header(null)
                .body(new JsonObject())
                .build();
    }

    @Test(expected = NullPointerException.class) public void testConstruct3() throws Exception {
        Packet.builder()
                .header(Packet.Header.builder().build())
                .body(null)
                .build();
    }

    @Test public void testConstruct4() throws Exception {
        createPacket();
    }

    @Test public void testHeaderSource1() throws Exception {
        assertEquals(TEST_ROUTE, createPacket().getHeader().getSource());
    }

    @Test public void testHeaderSource2() throws Exception {
        Packet.Header headers = createPacket().getHeader();
        headers.setSource(TEST_ROUTE + "_mod");
        assertEquals(TEST_ROUTE + "_mod", headers.getSource());
    }

    @Test public void testHeaderRoute1() throws Exception {
        assertEquals(TEST_ROUTE, createPacket().getHeader().getRoute());
    }

    @Test public void testHeaderRoute2() throws Exception {
        Packet.Header headers = createPacket().getHeader();
        headers.setRoute(TEST_ROUTE + "_mod");
        assertEquals(TEST_ROUTE + "_mod", headers.getRoute());
    }

    @Test public void testHeaderOldRoute1() throws Exception {
        assertEquals(null, createPacket().getHeader().getOldRoute());
    }

    @Test public void testHeaderOldRoute2() throws Exception {
        Packet.Header headers = createPacket().getHeader();
        headers.setOldRoute(TEST_ROUTE + "_mod");
        assertEquals(TEST_ROUTE + "_mod", headers.getOldRoute());
    }

    @Test public void testPrevious1() throws Exception {
        assertEquals(-1, createPacket().getHeader().getPrevious());
    }

    @Test public void testPrevious2() throws Exception {
        Packet.Header headers = createPacket().getHeader();
        headers.setPrevious(0);
        assertEquals(0, headers.getPrevious());
    }

    @Test public void testCurrent1() throws Exception {
        assertEquals(0, createPacket().getHeader().getCurrent());
    }

    @Test public void testCurrent2() throws Exception {
        Packet.Header headers = createPacket().getHeader();
        headers.setCurrent(1);
        assertEquals(1, headers.getCurrent());
    }

    @Test public void testCopy1() {
        Packet packet = createPacket();
        assertFalse(packet == packet.copy());
    }

    @Test public void testCopy2() {
        Packet packet = createPacket();
        Packet copy = packet.copy();
        packet.getHeader().setCurrent(-1);
        assertTrue(packet.getHeader().getCurrent() != copy.getHeader().getCurrent());
    }

    @Test public void testCopy3() {
        Packet packet = createPacket();
        packet.getHeader().put("message", "foobar");
        Packet copy = packet.copy();
        packet.getHeader().put("message", "foobar_mod");
        assertEquals("foobar", copy.getHeader().getString("message"));
    }

    private Packet createPacket() {
        return Packet.builder()
                .header(Packet.Header.builder()
                        .source(TEST_ROUTE)
                        .route("foobar")
                        .build())
                .body(new JsonObject())
                .build();
    }

}