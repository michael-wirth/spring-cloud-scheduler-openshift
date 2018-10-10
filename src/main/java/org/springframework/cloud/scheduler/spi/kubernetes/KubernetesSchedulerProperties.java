/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.scheduler.spi.kubernetes;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * Configuration properties for the Kubernetes Scheduler.
 *
 * @author Chris Schaefer
 */
@ConfigurationProperties(prefix = KubernetesSchedulerProperties.KUBERNETES_SCHEDULER_PROPERTIES)
public class KubernetesSchedulerProperties {
	/**
	 * Namespace to use for Kubernetes Scheduler properties.
	 */
	public static final String KUBERNETES_SCHEDULER_PROPERTIES = "spring.cloud.scheduler.kubernetes";

	/**
	 * The "default" Kubernetes namespace.
	 */
	private static final String DEFAULT_KUBERNETES_NAMESPACE = "default";

	/**
	 * Name of the environment variable that can define the Kubernetes namespace to use.
	 */
	protected static final String ENV_KEY_KUBERNETES_NAMESPACE = "KUBERNETES_NAMESPACE";

	/**
	 * The Kubernetes namespace obtained from the environment variable
	 * {@link KubernetesSchedulerProperties#ENV_KEY_KUBERNETES_NAMESPACE} if any.
	 */
	private static final String ENV_KUBERNETES_NAMESPACE = System.getenv(ENV_KEY_KUBERNETES_NAMESPACE);

	/**
	 * The Kubernetes namespace to use. First check the environment value of
	 * {@link KubernetesSchedulerProperties#ENV_KUBERNETES_NAMESPACE}, if not present, use
	 * {@link KubernetesSchedulerProperties#DEFAULT_KUBERNETES_NAMESPACE}.
	 */
	private static String KUBERNETES_NAMESPACE = StringUtils.hasText(ENV_KUBERNETES_NAMESPACE)
			? ENV_KUBERNETES_NAMESPACE
			: DEFAULT_KUBERNETES_NAMESPACE;

	/**
	 * The {@link ImagePullPolicy} to use. Defaults to {@link ImagePullPolicy#IfNotPresent}.
	 */
	private ImagePullPolicy imagePullPolicy = ImagePullPolicy.IfNotPresent;

	/**
	 * The {@link RestartPolicy} to use. Defaults to {@link RestartPolicy#Never}.
	 */
	private RestartPolicy restartPolicy = RestartPolicy.Never;

	/**
	 * The {@link EntryPointStyle} to use. Defaults to {@link EntryPointStyle#exec}.
	 */
	private EntryPointStyle entryPointStyle = EntryPointStyle.exec;

	/**
	 * The Kubernetes namespace to use. Defaults to
	 * {@link KubernetesSchedulerProperties#KUBERNETES_NAMESPACE}.
	 */
	private String namespace = KUBERNETES_NAMESPACE;

	/**
	 * Obtains the {@link ImagePullPolicy} to use. Defaults to
	 * {@link KubernetesSchedulerProperties#imagePullPolicy}.
	 *
	 * @return the {@link ImagePullPolicy} to use
	 */
	public ImagePullPolicy getImagePullPolicy() {
		return imagePullPolicy;
	}

	/**
	 * Sets the {@link ImagePullPolicy} to use.
	 *
	 * @param imagePullPolicy the {@link ImagePullPolicy} to use
	 */
	public void setImagePullPolicy(ImagePullPolicy imagePullPolicy) {
		this.imagePullPolicy = imagePullPolicy;
	}

	/**
	 * Obtains the {@link RestartPolicy} to use. Defaults to
	 * {@link KubernetesSchedulerProperties#restartPolicy}.
	 *
	 * @return the {@link RestartPolicy} to use
	 */
	public RestartPolicy getRestartPolicy() {
		return restartPolicy;
	}

	/**
	 * Sets the {@link RestartPolicy} to use.
	 *
	 * @param restartPolicy the {@link RestartPolicy} to use
	 */
	public void setRestartPolicy(RestartPolicy restartPolicy) {
		this.restartPolicy = restartPolicy;
	}

	/**
	 * Obtains the {@link EntryPointStyle} to use. Defaults to
	 * {@link KubernetesSchedulerProperties#entryPointStyle}.
	 *
	 * @return the {@link EntryPointStyle} to use
	 */
	public EntryPointStyle getEntryPointStyle() {
		return entryPointStyle;
	}

	/**
	 * Sets the {@link EntryPointStyle} to use.
	 *
	 * @param entryPointStyle the {@link EntryPointStyle} to use
	 */
	public void setEntryPointStyle(EntryPointStyle entryPointStyle) {
		this.entryPointStyle = entryPointStyle;
	}

	/**
	 * Obtains the Kubernetes namespace to use. Defaults to
	 * {@link KubernetesSchedulerProperties#namespace}.
	 *
	 * @return the Kubernetes namespace to use
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * Sets the Kubernetes namespace to use.
	 *
	 * @param namespace the Kubernetes namespace to use
	 */
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
}