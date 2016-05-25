package com.example;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;

@EnableTask
@EnableBatchProcessing
@SpringBootApplication
public class Batch101Application {

	@Bean
	Job hello(JobBuilderFactory factory, StepBuilderFactory steps) {
		return factory
				.get("hw")
				.start(steps.get("one")
						.tasklet((stepContribution, chunkContext) -> {
							System.out.println("Hello, world");
							System.out.println("parameters: ");
							chunkContext.getStepContext().getJobParameters()
									.entrySet()
									.forEach(e -> System.out.println(e.getKey() + ':' + e.getValue()));
							return RepeatStatus.FINISHED;
						}).build())
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(Batch101Application.class, args);
	}
}
