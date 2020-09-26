/**
 * Copyright 2016-2020 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.amqp.internal.stream;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.CLOSE_PIPE;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.CLOSE_RCVD;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.CLOSE_SENT;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.DISCARDING;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.END;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.HDR_EXCH;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.HDR_RCVD;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.HDR_SENT;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.OC_PIPE;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.OPENED;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.OPEN_PIPE;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.OPEN_RCVD;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.OPEN_SENT;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpConnectionState.START;

import org.junit.Test;

public class AmqpConnectionStateTest
{
    @Test
    public void shouldTransitionFromStartToHeaderReceived() throws Exception
    {
        AmqpConnectionState state = START.receivedHeader();
        assertEquals(HDR_RCVD, state);
    }

    @Test
    public void shouldTransitionFromStartToHeaderSent() throws Exception
    {
        AmqpConnectionState state = START.sentHeader();
        assertEquals(HDR_SENT, state);
    }

    @Test
    public void shouldTransitionFromHeaderReceivedToHeaderExchanged() throws Exception
    {
        AmqpConnectionState state = HDR_RCVD.sentHeader();
        assertEquals(HDR_EXCH, state);
    }

    @Test
    public void shouldTransitionFromHeaderSentToHeaderExchanged() throws Exception
    {
        AmqpConnectionState state = HDR_SENT.receivedHeader();
        assertEquals(HDR_EXCH, state);
    }

    @Test
    public void shouldTransitionFromHeaderExchangedToOpenReceived() throws Exception
    {
        AmqpConnectionState state = HDR_EXCH.receivedOpen();
        assertEquals(OPEN_RCVD, state);
    }

    @Test
    public void shouldTransitionFromHeaderExchangedToOpenSent() throws Exception
    {
        AmqpConnectionState state = HDR_EXCH.sentOpen();
        assertEquals(OPEN_SENT, state);
    }

    @Test
    public void shouldTransitionFromOpenReceivedToOpened() throws Exception
    {
        AmqpConnectionState state = OPEN_RCVD.sentOpen();
        assertEquals(OPENED, state);
    }

    @Test
    public void shouldTransitionFromOpenSentToOpened() throws Exception
    {
        AmqpConnectionState state = OPEN_SENT.receivedOpen();
        assertEquals(OPENED, state);
    }

    @Test
    public void shouldTransitionFromOpenedToCloseReceived() throws Exception
    {
        AmqpConnectionState state = OPENED.receivedClose();
        assertEquals(CLOSE_RCVD, state);
    }

    @Test
    public void shouldTransitionFromOpenedToDiscarding() throws Exception
    {
        AmqpConnectionState state = OPENED.sentClose();
        assertEquals(DISCARDING, state);
    }

    @Test
    public void shouldTransitionFromCloseReceivedToEnd() throws Exception
    {
        AmqpConnectionState state = CLOSE_RCVD.sentClose();
        assertEquals(END, state);
    }

    @Test
    public void shouldTransitionFromDiscardingToEnd() throws Exception
    {
        AmqpConnectionState state = DISCARDING.receivedClose();
        assertEquals(END, state);
    }

    @Test
    public void shouldTransitionFromCloseSentToEnd() throws Exception
    {
        AmqpConnectionState state = CLOSE_SENT.receivedClose();
        assertEquals(END, state);
    }

    @Test
    public void shouldTransitionFromHeaderSentToOpenPipelined() throws Exception
    {
        AmqpConnectionState state = HDR_SENT.sentOpen();
        assertEquals(OPEN_PIPE, state);
    }

    @Test
    public void shouldTransitionFromOpenPipelinedToOpenSent() throws Exception
    {
        AmqpConnectionState state = OPEN_PIPE.receivedHeader();
        assertEquals(OPEN_SENT, state);
    }

    @Test
    public void shouldTransitionFromOpenPipelinedToOpenClosePipelined() throws Exception
    {
        AmqpConnectionState state = OPEN_PIPE.sentClose();
        assertEquals(OC_PIPE, state);
    }

    @Test
    public void shouldTransitionFromOpenClosePipelinedToClosePipelined() throws Exception
    {
        AmqpConnectionState state = OC_PIPE.receivedHeader();
        assertEquals(CLOSE_PIPE, state);
    }

    @Test
    public void shouldTransitionFromOpenSentToClosePipelined() throws Exception
    {
        AmqpConnectionState state = OPEN_SENT.sentClose();
        assertEquals(CLOSE_PIPE, state);
    }

    @Test
    public void shouldTransitionFromClosePipelinedToCloseSent() throws Exception
    {
        AmqpConnectionState state = CLOSE_PIPE.receivedOpen();
        assertEquals(CLOSE_SENT, state);
    }
}
