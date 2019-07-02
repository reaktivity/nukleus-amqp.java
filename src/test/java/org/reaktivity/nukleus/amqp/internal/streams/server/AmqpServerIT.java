/**
 * Copyright 2016-2019 The Reaktivity Project
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
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

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

    @Ignore
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

    @Ignore
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

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.client.at.most.once/client",
        "${server}/send.to.client.at.most.once/server" })
    public void shouldSendToClientAtMostOnce() throws Exception
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

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/link/transfer.to.server.at.most.once/client",
        "${server}/send.to.server.at.most.once/server" })
    public void shouldSendToServerAtMostOnce() throws Exception
    {
        k3po.finish();
    }
}
