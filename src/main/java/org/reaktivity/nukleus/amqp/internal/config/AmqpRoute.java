/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.amqp.internal.config;

import static java.util.stream.Collectors.toList;

import java.util.List;

import org.reaktivity.reaktor.config.Options;
import org.reaktivity.reaktor.config.Route;

public final class AmqpRoute extends Options
{
    public final long id;
    public final List<AmqpMatcher> when;

    public AmqpRoute(
        Route route)
    {
        this.id = route.id;
        this.when = route.when.stream()
            .map(AmqpCondition.class::cast)
            .map(AmqpMatcher::new)
            .collect(toList());
    }
}
