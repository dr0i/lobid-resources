/*
 *  Copyright 2014 hbz, Pascal Christoph
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.lobid.lodmill;

import org.culturegraph.mf.morph.functions.AbstractSimpleStatelessFunction;

import com.google.common.net.UrlEscapers;

/**
 * @author Pascal Christoph (dr0i)
 */
public final class URLEscaper extends AbstractSimpleStatelessFunction {
	@Override
	public String process(final String value) {
		return UrlEscapers.urlFragmentEscaper().escape(value);
	}
}
