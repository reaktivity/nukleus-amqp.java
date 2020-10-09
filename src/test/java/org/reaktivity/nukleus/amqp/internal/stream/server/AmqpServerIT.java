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
package org.reaktivity.nukleus.amqp.internal.stream.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.amqp.internal.AmqpConfiguration.AMQP_CLOSE_EXCHANGE_TIMEOUT;
import static org.reaktivity.nukleus.amqp.internal.AmqpConfiguration.AMQP_CONTAINER_ID;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configure;

public class AmqpServerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/amqp/control/route")
        .addScriptRoot("client", "org/reaktivity/specification/amqp")
        .addScriptRoot("server", "org/reaktivity/specification/nukleus/amqp/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .nukleus("amqp"::equals)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .configure(AMQP_CONTAINER_ID, "server")
        .configure(ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE, false)
        .configure(AMQP_CLOSE_EXCHANGE_TIMEOUT, 500)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);


    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/header.exchange/handshake.client" })
    public void shouldExchangeHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/sasl.exchange/client" })
    public void shouldExchangeSasl() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/protocol.header.unmatched/client" })
    public void shouldCloseStreamWhenHeaderUnmatched() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/open.exchange/client" })
    public void shouldExchangeOpen() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/close.exchange/client" })
    public void shouldExchangeClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/close.exchange.server.abandoned/client" })
    @Configure(name = "nukleus.amqp.idle.timeout", value = "1000")
    public void shouldCloseStreamWhenServerAbandoned() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/begin.exchange/client" })
    public void shouldExchangeBegin() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/begin.then.close/client" })
    public void shouldExchangeBeginThenClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/end.exchange/client" })
    public void shouldExchangeEnd() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/attach.as.receiver.only/client",
        "${server}/connect.as.receiver.only/server" })
    public void shouldConnectAsReceiverOnly() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/attach.as.receiver.then.sender/client",
        "${server}/connect.as.receiver.then.sender/server" })
    public void shouldConnectAsReceiverThenSender() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/attach.as.sender.only/client",
        "${server}/connect.as.sender.only/server" })
    public void shouldConnectAsSenderOnly() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/attach.as.sender.then.receiver/client",
        "${server}/connect.as.sender.then.receiver/server" })
    public void shouldConnectAsSenderThenReceiver() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/detach.exchange/client",
        "${server}/disconnect/server" })
    public void shouldExchangeDetach() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/attach.as.receiver.when.source.does.not.exist/client",
        "${server}/connect.and.reset/server" })
    public void shouldConnectAsReceiverAndReset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/attach.as.sender.when.target.does.not.exist/client",
        "${server}/connect.and.reset/server" })
    public void shouldConnectAsSenderAndReset() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.at.least.once/client",
        "${server}/send.to.client.at.least.once/server" })
    public void shouldSendToClientAtLeastOnce() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.array8/client",
        "${server}/send.to.client.with.array8/server" })
    public void shouldSendToClientWithArray8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.array32/client",
        "${server}/send.to.client.with.array32/server" })
    public void shouldSendToClientWithArray32() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.boolean/client",
        "${server}/send.to.client.with.boolean/server" })
    public void shouldSendToClientWithBoolean() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.byte/client",
        "${server}/send.to.client.with.byte/server" })
    public void shouldSendToClientWithByte() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.char/client",
        "${server}/send.to.client.with.char/server" })
    public void shouldSendToClientWithChar() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.false/client",
        "${server}/send.to.client.with.false/server" })
    public void shouldSendToClientWithFalse() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.int/client",
        "${server}/send.to.client.with.int/server" })
    public void shouldSendToClientWithInt() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.list0/client",
        "${server}/send.to.client.with.list0/server" })
    public void shouldSendToClientWithList0() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.list8/client",
        "${server}/send.to.client.with.list8/server" })
    public void shouldSendToClientWithList8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.list32/client",
        "${server}/send.to.client.with.list32/server" })
    public void shouldSendToClientWithList32() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.long/client",
        "${server}/send.to.client.with.long/server" })
    public void shouldSendToClientWithLong() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.map8/client",
        "${server}/send.to.client.with.map8/server" })
    public void shouldSendToClientWithMap8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.map32/client",
        "${server}/send.to.client.with.map32/server" })
    public void shouldSendToClientWithMap32() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.multiple.data/client",
        "${server}/send.to.client.with.multiple.data/server" })
    public void shouldSendToClientWithMultipleData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.multiple.sequence/client",
        "${server}/send.to.client.with.multiple.sequence/server" })
    public void shouldSendToClientWithMultipleSequence() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.null/client",
        "${server}/send.to.client.with.null/server" })
    public void shouldSendToClientWithNull() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.short/client",
        "${server}/send.to.client.with.short/server" })
    public void shouldSendToClientWithShort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.single.data/client",
        "${server}/send.to.client.with.single.data/server" })
    public void shouldSendToClientWithSingleData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.single.sequence/client",
        "${server}/send.to.client.with.single.sequence/server" })
    public void shouldSendToClientWithSingleSequence() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.smallint/client",
        "${server}/send.to.client.with.smallint/server" })
    public void shouldSendToClientWithSmallInt() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.smalllong/client",
        "${server}/send.to.client.with.smalllong/server" })
    public void shouldSendToClientWithSmallLong() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.smalluint/client",
        "${server}/send.to.client.with.smalluint/server" })
    public void shouldSendToClientWithSmallUint() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.smallulong/client",
        "${server}/send.to.client.with.smallulong/server" })
    public void shouldSendToClientWithSmallUlong() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.str8utf8/client",
        "${server}/send.to.client.with.str8utf8/server" })
    public void shouldSendToClientWithStr8utf8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.str32utf8/client",
        "${server}/send.to.client.with.str32utf8/server" })
    public void shouldSendToClientWithStr32utf8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.sym8/client",
        "${server}/send.to.client.with.sym8/server" })
    public void shouldSendToClientWithSym8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.sym32/client",
        "${server}/send.to.client.with.sym32/server" })
    public void shouldSendToClientWithSym32() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.timestamp/client",
        "${server}/send.to.client.with.timestamp/server" })
    public void shouldSendToClientWithTimestamp() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.true/client",
        "${server}/send.to.client.with.true/server" })
    public void shouldSendToClientWithTrue() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.ubyte/client",
        "${server}/send.to.client.with.ubyte/server" })
    public void shouldSendToClientWithUbyte() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.uint/client",
        "${server}/send.to.client.with.uint/server" })
    public void shouldSendToClientWithUint() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.uint0/client",
        "${server}/send.to.client.with.uint0/server" })
    public void shouldSendToClientWithUint0() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.ulong/client",
        "${server}/send.to.client.with.ulong/server" })
    public void shouldSendToClientWithUlong() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.ulong0/client",
        "${server}/send.to.client.with.ulong0/server" })
    public void shouldSendToClientWithUlong0() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.ushort/client",
        "${server}/send.to.client.with.ushort/server" })
    public void shouldSendToClientWithUshort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.vbin8/client",
        "${server}/send.to.client.with.vbin8/server" })
    public void shouldSendToClientWithVbin8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.vbin32/client",
        "${server}/send.to.client.with.vbin32/server" })
    public void shouldSendToClientWithVbin32() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.array8/client",
        "${server}/send.to.server.with.array8/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithArray8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.array32/client",
        "${server}/send.to.server.with.array32/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithArray32() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.boolean/client",
        "${server}/send.to.server.with.boolean/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithBoolean() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.byte/client",
        "${server}/send.to.server.with.byte/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithByte() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.char/client",
        "${server}/send.to.server.with.char/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithChar() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.false/client",
        "${server}/send.to.server.with.false/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithFalse() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.int/client",
        "${server}/send.to.server.with.int/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithInt() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.list0/client",
        "${server}/send.to.server.with.list0/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithList0() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.list8/client",
        "${server}/send.to.server.with.list8/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithlist8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.list32/client",
        "${server}/send.to.server.with.list32/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithList32() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.long/client",
        "${server}/send.to.server.with.long/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithLong() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.map8/client",
        "${server}/send.to.server.with.map8/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithMap8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.map32/client",
        "${server}/send.to.server.with.map32/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithMap32() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.multiple.data/client",
        "${server}/send.to.server.with.multiple.data/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithMultipleData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.multiple.sequence/client",
        "${server}/send.to.server.with.multiple.sequence/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithMultipleSequence() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.null/client",
        "${server}/send.to.server.with.null/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithNull() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.properties/client",
        "${server}/send.to.server.with.properties/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithProperties() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.short/client",
        "${server}/send.to.server.with.short/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithShort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.single.data/client",
        "${server}/send.to.server.with.single.data/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithSingleData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.single.sequence/client",
        "${server}/send.to.server.with.single.sequence/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithSingleSequence() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.smallint/client",
        "${server}/send.to.server.with.smallint/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithSmallInt() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.smalllong/client",
        "${server}/send.to.server.with.smalllong/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithSmallLong() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.smalluint/client",
        "${server}/send.to.server.with.smalluint/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithSmallUint() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.smallulong/client",
        "${server}/send.to.server.with.smallulong/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithSmallUlong() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.str8utf8/client",
        "${server}/send.to.server.with.str8utf8/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithStr8utf8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.str32utf8/client",
        "${server}/send.to.server.with.str32utf8/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithStr32utf8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.sym8/client",
        "${server}/send.to.server.with.sym8/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithSym8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.sym32/client",
        "${server}/send.to.server.with.sym32/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithSym32() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.timestamp/client",
        "${server}/send.to.server.with.timestamp/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithTimestamp() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.true/client",
        "${server}/send.to.server.with.true/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithTrue() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.ubyte/client",
        "${server}/send.to.server.with.ubyte/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithUbyte() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.uint/client",
        "${server}/send.to.server.with.uint/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithUint() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.uint0/client",
        "${server}/send.to.server.with.uint0/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithUint0() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.ulong/client",
        "${server}/send.to.server.with.ulong/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithUlong() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.ulong0/client",
        "${server}/send.to.server.with.ulong0/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithUlong0() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.ushort/client",
        "${server}/send.to.server.with.ushort/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithUshort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.vbin8/client",
        "${server}/send.to.server.with.vbin8/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithVbin8() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.vbin32/client",
        "${server}/send.to.server.with.vbin32/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithVbin32() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.at.least.once/client",
        "${server}/send.to.server.at.least.once/server" })
    public void shouldSendToServerAtLeastOnce() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.headers/client",
        "${server}/send.to.server.with.headers/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithHeaders() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.delivery.annotations/client",
        "${server}/send.to.server.with.delivery.annotations/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithDeliveryAnnotations() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.annotations/client",
        "${server}/send.to.server.with.annotations/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithAnnotations() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.application.properties/client",
        "${server}/send.to.server.with.application.properties/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithApplicationProperties() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.footer/client",
        "${server}/send.to.server.with.footer/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWithFooter() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.when.max.frame.size.exceeded/client",
        "${server}/send.to.server.when.max.frame.size.exceeded/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "1000")
    public void shouldSendToServerWhenMaxFrameSizeExceeded() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.when.fragmented/client",
        "${server}/send.to.server.when.fragmented/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8000")
    public void shouldSendToServerWhenFragmented() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.when.links.interleaved/client",
        "${server}/send.to.server.when.links.interleaved/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8000")
    public void shouldSendToServerWhenLinksInterleaved() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.when.links.interleaved.and.fragmented/client",
        "${server}/send.to.server.when.links.interleaved.and.fragmented/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8000")
    public void shouldSendToServerWhenLinksInterleavedAndFragmented() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/transfer.to.server.when.sessions.interleaved/client",
        "${server}/send.to.server.when.sessions.interleaved/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8000")
    public void shouldSendToServerWhenSessionsInterleaved() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/transfer.to.server.when.sessions.interleaved.and.fragmented/client",
        "${server}/send.to.server.when.sessions.interleaved.and.fragmented/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8000")
    public void shouldSendToServerWhenSessionsInterleavedAndFragmented() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/incoming.window.exceeded/client",
        "${server}/incoming.window.exceeded/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8192")
    public void shouldEndSessionWhenIncomingWindowExceeded() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/send.to.client.multiple.sessions/client",
        "${server}/send.to.client.through.multiple.sessions/server" })
    public void shouldSendToClientThroughMultipleSessions() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.annotations/client",
        "${server}/send.to.client.with.annotations/server" })
    public void shouldSendToClientWithAnnotations() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.properties/client",
        "${server}/send.to.client.with.properties/server" })
    public void shouldSendToClientWithProperties() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.with.application.properties/client",
        "${server}/send.to.client.with.application.properties/server" })
    public void shouldSendToClientWithApplicationProperties() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.when.max.frame.size.exceeded/client",
        "${server}/send.to.client.when.max.frame.size.exceeded/server" })
    public void shouldSendToClientWhenMaxFrameSizeExceeded() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.when.fragmented/client",
        "${server}/send.to.client.when.fragmented/server" })
    public void shouldSendToClientWhenFragmented() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.when.links.interleaved/client",
        "${server}/send.to.client.when.links.interleaved/server" })
    public void shouldSendToClientWhenLinksInterleaved() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.when.links.interleaved.and.max.frame.size.exceeded/client",
        "${server}/send.to.client.when.links.interleaved.and.max.frame.size.exceeded/server" })
    public void shouldSendToClientWhenLinksInterleavedAndMaxFrameSizeExceeded() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.when.links.interleaved.and.fragmented/client",
        "${server}/send.to.client.when.links.interleaved.and.fragmented/server" })
    public void shouldSendToClientWhenLinksInterleavedAndFragmented() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/transfer.to.client.when.sessions.interleaved/client",
        "${server}/send.to.client.when.sessions.interleaved/server" })
    public void shouldSendToClientWhenSessionsInterleaved() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/transfer.to.client.when.sessions.interleaved.and.max.frame.size.exceeded/client",
        "${server}/send.to.client.when.sessions.interleaved.and.max.frame.size.exceeded/server" })
    public void shouldSendToClientWhenSessionsInterleavedAndMaxFrameSizeExceeded() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/transfer.to.client.when.sessions.interleaved.and.fragmented/client",
        "${server}/send.to.client.when.sessions.interleaved.and.fragmented/server" })
    public void shouldSendToClientWhenSessionsInterleavedAndFragmented() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/link.credit.exceeded/client",
        "${server}/link.credit.exceeded/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8192")
    public void shouldDetachLinkWhenLinkCreditExceeded() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/max.frame.size.exceeded.with.multiple.sessions.and.links/client",
        "${server}/max.frame.size.exceeded.with.multiple.sessions.and.links/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8000")
    public void shouldCloseConnectionWhenMaxFrameSizeExceededWithMultipleSessions() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/server.idle.timeout.expires/client" })
    @Configure(name = "nukleus.amqp.idle.timeout", value = "1000")
    public void shouldCloseConnectionWithTimeout() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/server.idle.timeout.does.not.expire/client" })
    @Configure(name = "nukleus.amqp.idle.timeout", value = "1000")
    public void shouldPreventTimeoutSentByServer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/client.idle.timeout.does.not.expire/client" })
    public void shouldPreventTimeoutSentByClient() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/incoming.window.reduced/client",
        "${server}/incoming.window.reduced/server" })
    public void shouldHandleReducedIncomingWindow() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/open.exchange.pipelined/client" })
    public void shouldExchangeOpenPipelined() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/sasl.exchange.then.open.exchange.pipelined/client" })
    public void shouldExchangeOpenPipelinedAfterSasl() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/close.exchange.simultaneous/client" })
    @Configure(name = "nukleus.amqp.idle.timeout", value = "1000")
    public void shouldExchangeCloseSimultaneously() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/end.exchange.simultaneous/client",
        "${server}/incoming.window.exceeded/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8192")
    public void shouldEndSessionSimultaneously() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/discard.after.end/client",
        "${server}/incoming.window.exceeded/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8192")
    public void shouldDiscardInboundAfterOutboundEnd() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/reject.errant.link/client",
        "${server}/link.credit.exceeded/server" })
    @Configure(name = "nukleus.amqp.max.frame.size", value = "8192")
    public void shouldRejectErrantLinks() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/open.with.outgoing.locales/client" })
    public void shouldSendOpenWithOutgoingLocales() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection/reject.open.with.outgoing.locales.when.enus.omitted/client" })
    public void shouldSendOpenWithOutgoingLocalesWhenEnUsOmitted() throws Exception
    {
        k3po.finish();
    }
}
