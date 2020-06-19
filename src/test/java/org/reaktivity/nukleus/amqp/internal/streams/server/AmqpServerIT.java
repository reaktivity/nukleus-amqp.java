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
package org.reaktivity.nukleus.amqp.internal.streams.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.amqp.internal.AmqpConfiguration.AMQP_CONTAINER_ID;
import static org.reaktivity.nukleus.amqp.internal.AmqpConfiguration.AMQP_MAX_FRAME_SIZE;
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
        .configure(AMQP_MAX_FRAME_SIZE, 131072L)
        .configure(ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE, false)
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
        "${client}/session/begin.exchange/client" })
    public void shouldExchangeBegin() throws Exception
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

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/detach.link/client",
        "${server}/disconnect.abort/server" })
    public void shouldDisconnectWithAbort() throws Exception
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
        "${client}/link/transfer.to.client/client",
        "${server}/send.to.client/server" })
    public void shouldSendToClient() throws Exception
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
        "${client}/link/transfer.to.server/client",
        "${server}/send.to.server/server" })
    public void shouldSendToServer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.annotations/client",
        "${server}/send.to.server.with.annotations/server" })
    public void shouldSendToServerWithAnnotations() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.application.properties/client",
        "${server}/send.to.server.with.application.properties/server" })
    public void shouldSendToServerWithApplicationProperties() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.with.properties/client",
        "${server}/send.to.server.with.properties/server" })
    public void shouldSendToServerWithProperties() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.when.fragmented/client",
        "${server}/send.to.server.when.fragmented/server" })
    @Configure(name = "max.frame.size", value = "500")
    public void shouldSendToServerWhenFragmented() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/incoming.window.exceeded/client",
        "${server}/incoming.window.exceeded/server" })
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

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/session/transfer.to.client.when.sessions.interleaved.and.fragmented/client",
        "${server}/send.to.client.when.sessions.interleaved.and.fragmented/server" })
    public void shouldSendToClientWhenSessionsInterleavedAndFragmented() throws Exception
    {
        k3po.finish();
    }
}
