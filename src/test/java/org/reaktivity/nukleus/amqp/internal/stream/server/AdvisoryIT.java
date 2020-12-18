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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;

public class AdvisoryIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/amqp/control/route")
        .addScriptRoot("client", "org/reaktivity/specification/amqp/link")
        .addScriptRoot("server", "org/reaktivity/specification/nukleus/amqp/streams");;

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(2048)
        .responseBufferCapacity(2048)
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
        "${client}/server.sent.flush/client",
        "${server}/server.sent.flush/server" })
    public void shouldReceiveServerSentFlush() throws Exception
    {
        k3po.finish();
    }
}
