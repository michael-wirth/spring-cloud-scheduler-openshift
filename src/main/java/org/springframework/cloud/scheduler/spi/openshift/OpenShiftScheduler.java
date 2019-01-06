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

package org.springframework.cloud.scheduler.spi.openshift;

import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.StatusCause;
import io.fabric8.kubernetes.api.model.batch.CronJob;
import io.fabric8.kubernetes.api.model.batch.CronJobBuilder;
import io.fabric8.kubernetes.api.model.batch.CronJobList;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.openshift.client.OpenShiftClient;
import org.springframework.cloud.deployer.resource.docker.DockerResource;
import org.springframework.cloud.deployer.resource.maven.MavenProperties;
import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.deployer.spi.kubernetes.AbstractKubernetesDeployer;
import org.springframework.cloud.deployer.spi.kubernetes.ContainerFactory;
import org.springframework.cloud.deployer.spi.openshift.OpenShiftDeployerProperties;
import org.springframework.cloud.deployer.spi.openshift.ResourceHash;
import org.springframework.cloud.deployer.spi.openshift.maven.MavenOpenShiftTaskLauncher;
import org.springframework.cloud.deployer.spi.openshift.maven.MavenResourceJarExtractor;
import org.springframework.cloud.scheduler.spi.core.*;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Kubernetes implementation of the {@link Scheduler} SPI.
 *
 * @author Chris Schaefer
 */
public class OpenShiftScheduler extends AbstractKubernetesDeployer implements Scheduler {

	private static final String SPRING_CRONJOB_ID_KEY = "spring-cronjob-id";

	private static final String SCHEDULE_EXPRESSION_FIELD_NAME = "spec.schedule";

	private final MavenProperties mavenProperties;

	private final MavenResourceJarExtractor mavenResourceJarExtractor;

	private final ResourceHash resourceHash;

	public OpenShiftScheduler(OpenShiftClient openShiftClient,
			OpenShiftDeployerProperties openShiftDeployerProperties,
			MavenProperties mavenProperties,
			MavenResourceJarExtractor mavenResourceJarExtractor,
			ResourceHash resourceHash, ContainerFactory containerFactory) {
		Assert.notNull(openShiftClient, "OpenShiftClient must not be null");
		Assert.notNull(openShiftDeployerProperties,
				"KubernetesSchedulerProperties must not be null");

		this.client = openShiftClient;
		this.properties = openShiftDeployerProperties;
		this.mavenProperties = mavenProperties;
		this.mavenResourceJarExtractor = mavenResourceJarExtractor;
		this.resourceHash = resourceHash;
		this.containerFactory = containerFactory;
	}

	@Override
	public void schedule(ScheduleRequest scheduleRequest) {
		try {
			if (scheduleRequest.getResource() instanceof DockerResource) {
				createCronJob(scheduleRequest);
			}
			else {

				new MavenOpenShiftTaskLauncher((OpenShiftDeployerProperties) properties,
						mavenProperties, (OpenShiftClient) client,
						mavenResourceJarExtractor, resourceHash, containerFactory) {
					@Override
					protected String launchDockerResource(AppDeploymentRequest request) {
						ScheduleRequest updatedScheduleRequest = new ScheduleRequest(
								request.getDefinition(),
								scheduleRequest.getSchedulerProperties(),
								request.getDeploymentProperties(),
								request.getCommandlineArguments(),
								scheduleRequest.getScheduleName(), request.getResource());

						String appId = createDeploymentId(request);
						createCronJob(updatedScheduleRequest);
						return appId;
					}
				}.launch(scheduleRequest);
			}
		}
		catch (KubernetesClientException e) {
			String invalidCronExceptionMessage = getExceptionMessageForField(e,
					SCHEDULE_EXPRESSION_FIELD_NAME);

			if (StringUtils.hasText(invalidCronExceptionMessage)) {
				throw new CreateScheduleException(invalidCronExceptionMessage, e);
			}

			throw new CreateScheduleException(
					"Failed to create schedule " + scheduleRequest.getScheduleName(), e);
		}
	}

	@Override
	public void unschedule(String scheduleName) {
		boolean unscheduled = this.client.batch().cronjobs().withName(scheduleName)
				.delete();

		if (!unscheduled) {
			throw new SchedulerException(
					"Failed to unschedule schedule " + scheduleName + " does not exist.");
		}
	}

	@Override
	public List<ScheduleInfo> list(String taskDefinitionName) {
		return list().stream()
				.filter(scheduleInfo -> taskDefinitionName
						.equals(scheduleInfo.getTaskDefinitionName()))
				.collect(Collectors.toList());
	}

	@Override
	public List<ScheduleInfo> list() {
		CronJobList cronJobList = this.client.batch().cronjobs().list();

		List<CronJob> cronJobs = cronJobList.getItems();
		List<ScheduleInfo> scheduleInfos = new ArrayList<>();

		for (CronJob cronJob : cronJobs) {
			Map<String, String> properties = new HashMap<>();
			properties.put(SchedulerPropertyKeys.CRON_EXPRESSION,
					cronJob.getSpec().getSchedule());

			ScheduleInfo scheduleInfo = new ScheduleInfo();
			scheduleInfo.setScheduleName(cronJob.getMetadata().getName());
			scheduleInfo.setTaskDefinitionName(
					cronJob.getMetadata().getLabels().get(SPRING_CRONJOB_ID_KEY));
			scheduleInfo.setScheduleProperties(properties);

			scheduleInfos.add(scheduleInfo);
		}

		return scheduleInfos;
	}

	protected CronJob createCronJob(ScheduleRequest scheduleRequest) {
		Map<String, String> labels = Collections.singletonMap(SPRING_CRONJOB_ID_KEY,
				scheduleRequest.getDefinition().getName());

		String schedule = scheduleRequest.getSchedulerProperties()
				.get(SchedulerPropertyKeys.CRON_EXPRESSION);
		Assert.hasText(schedule, "The property: " + SchedulerPropertyKeys.CRON_EXPRESSION
				+ " must be defined");

		PodSpec podSpec = createPodSpec(scheduleRequest.getScheduleName(),
				scheduleRequest, null, true);

		CronJob cronJob = new CronJobBuilder().withNewMetadata()
				.withName(scheduleRequest.getScheduleName()).withLabels(labels)
				.endMetadata().withNewSpec().withSchedule(schedule).withNewJobTemplate()
				.withNewSpec().withNewTemplate().withNewSpec()
				.withImagePullSecrets(podSpec.getImagePullSecrets())
				.withServiceAccountName(podSpec.getServiceAccountName())
				.withContainers(podSpec.getContainers())
				.withRestartPolicy(podSpec.getRestartPolicy()).endSpec().endTemplate()
				.endSpec().endJobTemplate().endSpec().build();

		return this.client.batch().cronjobs().create(cronJob);
	}

	protected String getExceptionMessageForField(
			KubernetesClientException kubernetesClientException, String fieldName) {
		List<StatusCause> statusCauses = kubernetesClientException.getStatus()
				.getDetails().getCauses();

		if (!CollectionUtils.isEmpty(statusCauses)) {
			for (StatusCause statusCause : statusCauses) {
				if (fieldName.equals(statusCause.getField())) {
					return kubernetesClientException.getStatus().getMessage();
				}
			}
		}

		return null;
	}

}
