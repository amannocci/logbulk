/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016-2017
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
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test for Packet.
 */
public class PacketTest {

    private static final String TEST_ROUTE = "foobar";

    @Test(expected = NullPointerException.class)
    public void testConstruct1() throws Exception {
        Packet.builder()
                .header(null)
                .body(new JsonObject())
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testConstruct2() throws Exception {
        Packet.builder()
                .header(Packet.Header.builder()
                        .source(TEST_ROUTE)
                        .route("foobar").build())
                .body(null)
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testConstruct3() throws Exception {
        Packet.builder()
                .header(Packet.Header.builder()
                        .source(TEST_ROUTE)
                        .route("foobar")
                        .build())
                .body(null)
                .build();
    }

    @Test public void testConstruct5() throws Exception {
        Packet.builder()
                .header(Packet.Header.builder()
                        .source(TEST_ROUTE)
                        .route("foobar")
                        .build())
                .body(new JsonObject())
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testHeaderConstruct1() throws Exception {
        Packet.Header.builder()
                .source(null)
                .route("foobar")
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testHeaderConstruct2() throws Exception {
        Packet.Header.builder()
                .source(TEST_ROUTE)
                .route(null)
                .build();
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

    @Test public void testHeaderSetRoute1() throws Exception {
        Packet.Header header = createPacket().getHeader();
        assertNotEquals("random", header.getRoute());
        header.setRoute("random");
        assertEquals("random", header.getRoute());
    }

    @Test(expected = NullPointerException.class)
    public void testHeaderSetRoute2() throws Exception {
        Packet.Header header = createPacket().getHeader();
        header.setRoute(null);
    }

    @Test public void testHeaderSetSource1() throws Exception {
        Packet.Header header = createPacket().getHeader();
        assertNotEquals("random", header.getSource());
        header.setSource("random");
        assertEquals("random", header.getSource());
    }

    @Test(expected = NullPointerException.class)
    public void testHeaderSetSource2() throws Exception {
        Packet.Header header = createPacket().getHeader();
        header.setSource(null);
    }

    @Test public void testHeaderOldRoute1() throws Exception {
        assertNull(createPacket().getHeader().getOldRoute());
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

    @Test public void testHeaderEquals() {
        EqualsVerifier.forClass(Packet.Header.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .suppress(Warning.NULL_FIELDS)
                .verify();
    }

    @Test public void testEquals() {
        EqualsVerifier.forClass(Packet.class)
                .suppress(Warning.NONFINAL_FIELDS)
                .suppress(Warning.NULL_FIELDS)
                .verify();
    }


    @Test public void testHeaderToString1() {
        assertEquals("Packet.Header(source=foobar, route=foobar, oldRoute=null, previous=-1, current=0)", createPacket().getHeader().toString());
    }

    @Test public void testHeaderToString2() {
        assertEquals("Packet.Header.HeaderBuilder(source=null, route=null, oldRoute=null, previous=-1, current=0)", Packet.Header.builder().toString());
    }

    @Test public void testToString1() {
        assertEquals("Packet(header=Packet.Header(source=foobar, route=foobar, oldRoute=null, previous=-1, current=0), body={})", createPacket().toString());
    }

    @Test public void testToString2() {
        assertEquals("Packet.PacketBuilder(header=null, body=null)", Packet.builder().toString());
    }


    @Test public void testSetHeader1() {
        Packet.Header header = Packet.Header.builder()
                .source(TEST_ROUTE)
                .route("foobar_test")
                .build();
        Packet packet = createPacket();
        assertNotEquals(header, packet.getHeader());
        packet.setHeader(header);
        assertEquals(header, packet.getHeader());
    }

    @Test(expected = NullPointerException.class)
    public void testSetHeader2() {
        Packet packet = createPacket();
        packet.setHeader(null);
    }

    @Test public void testSetBody1() {
        JsonObject body = new JsonObject().put("test", "test");
        Packet packet = createPacket();
        assertNotEquals(body, packet.getBody());
        packet.setBody(body);
        assertEquals(body, packet.getBody());
    }

    @Test(expected = NullPointerException.class)
    public void testSetBody2() {
        Packet packet = createPacket();
        packet.setBody(null);
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