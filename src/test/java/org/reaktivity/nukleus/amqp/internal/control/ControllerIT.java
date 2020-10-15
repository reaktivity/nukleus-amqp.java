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
package org.reaktivity.nukleus.amqp.internal.control;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.route.RouteKind.CLIENT;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.amqp.internal.AmqpController;
import org.reaktivity.reaktor.test.ReaktorRule;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class ControllerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/amqp/control/route")
        .addScriptRoot("routeExt", "org/reaktivity/specification/nukleus/amqp/control/route.ext")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/amqp/control/unroute")
        .addScriptRoot("freeze", "org/reaktivity/specification/nukleus/control/freeze");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .controller("amqp"::equals);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(reaktor);

    private final Gson gson = new Gson();

    @Test
    @Specification({
        "${route}/server/nukleus"
    })
    public void shouldRouteServer() throws Exception
    {
        k3po.start();

        reaktor.controller(AmqpController.class)
                .route(SERVER, "amqp#0", "target#0")
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/send.only/server/nukleus"
    })
    public void shouldRouteServerSendOnly() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "clients");
        extension.addProperty("capabilities", "SEND_ONLY");

        reaktor.controller(AmqpController.class)
                .route(SERVER, "amqp#0", "target#0", gson.toJson(extension))
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/receive.only/server/nukleus"
    })
    public void shouldRouteServerReceiveOnly() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "clients/receive");
        extension.addProperty("capabilities", "RECEIVE_ONLY");

        reaktor.controller(AmqpController.class)
                .route(SERVER, "amqp#0", "target#0", gson.toJson(extension))
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/nukleus",
        "${unroute}/server/nukleus"
    })
    public void shouldUnrouteServer() throws Exception
    {
        k3po.start();

        long routeId = reaktor.controller(AmqpController.class)
                .route(SERVER, "amqp#0", "target#0")
                .get();

        k3po.notifyBarrier("ROUTED_SERVER");

        reaktor.controller(AmqpController.class)
                .unroute(routeId)
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/nukleus"
    })
    public void shouldRouteClient() throws Exception
    {
        k3po.start();

        reaktor.controller(AmqpController.class)
                .route(CLIENT, "amqp#0", "target#0")
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/send.only/client/nukleus"
    })
    public void shouldRouteClientSendOnly() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "clients");
        extension.addProperty("capabilities", "SEND_ONLY");

        reaktor.controller(AmqpController.class)
                .route(CLIENT, "amqp#0", "target#0", gson.toJson(extension))
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/receive.only/client/nukleus"
    })
    public void shouldRouteClientReceiveOnly() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "clients/receive");
        extension.addProperty("capabilities", "RECEIVE_ONLY");

        reaktor.controller(AmqpController.class)
                .route(CLIENT, "amqp#0", "target#0", gson.toJson(extension))
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/nukleus",
        "${unroute}/client/nukleus"
    })
    public void shouldUnrouteClient() throws Exception
    {
        k3po.start();

        long routeId = reaktor.controller(AmqpController.class)
                .route(CLIENT, "amqp#0", "target#0")
                .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(AmqpController.class)
                .unroute(routeId)
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${freeze}/nukleus"
    })
    @ScriptProperty("nameF00N \"amqp\"")
    public void shouldFreeze() throws Exception
    {
        k3po.start();

        reaktor.controller(AmqpController.class)
               .freeze()
               .get();

        k3po.finish();
    }
}
