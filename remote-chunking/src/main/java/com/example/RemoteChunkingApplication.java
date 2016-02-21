package com.example;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.RemoteChunkHandlerFactoryBean;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;

import javax.sql.DataSource;
import java.util.Collections;

import static com.example.BeanNames.*;

@SpringBootApplication
@EnableBatchProcessing
@IntegrationComponentScan
class RemoteChunkingApplication {

	static void main(String[] args) {
		SpringApplication.exit(SpringApplication.run(RemoteChunkingApplication.class, args));
	}
}

class BeanNames {

	public static final String ETL_STEP_1 = "step1";

	public static final String JDBC_ITEM_WRITER = "jdbcItemWriter";
	public static final String JDBC_ITEM_READER = "jdbcItemReader";

	private static final String REPLIES = "replies", REQUESTS = "requests", OUT = "out", IN = "in";

	public static final String MC_REQUESTS_IN = REQUESTS + IN,
			MC_REQUESTS_OUT = REQUESTS + OUT,
			MC_REPLIES_IN = REPLIES + IN,
			MC_REPLIES_OUT = REPLIES + OUT;

}

@Configuration
class BatchJobConfiguration {


	@Bean(name = JDBC_ITEM_READER)
	ItemReader<Customer> itemReader(DataSource dataSource) {

		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, firstName, lastName, birthdate");
		queryProvider.setFromClause("from CUSTOMER");
		queryProvider.setSortKeys(Collections.singletonMap("id", Order.ASCENDING));

		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();
		reader.setRowMapper((rs, i) -> new Customer(rs.getLong("id"), rs.getString("firstName"), rs.getString("lastName"), rs.getDate("birthdate")));
		reader.setQueryProvider(queryProvider);
		reader.setDataSource(dataSource);
		reader.setFetchSize(1000);

		return reader;
	}

	@Bean(name = JDBC_ITEM_WRITER)
	ItemWriter<Customer> itemWriter(DataSource dataSource) {
		JdbcBatchItemWriter<Customer> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(dataSource);
		itemWriter.setSql("INSERT INTO NEW_CUSTOMER VALUES (:id, :firstName, :lastName, :birthdate)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		return itemWriter;
	}

	@Bean(name = ETL_STEP_1)
	TaskletStep step1(StepBuilderFactory stepBuilderFactory,
	                  ItemReader<Customer> reader,
	                  ItemWriter<Customer> writer) {
		return stepBuilderFactory
				.get(ETL_STEP_1)
				.<Customer, Customer>chunk(1000)
				.reader(reader)
				.writer(writer)
				.build();
	}

	@Bean
	Job job(JobBuilderFactory jobBuilderFactory,
	        @Qualifier(ETL_STEP_1) Step step1) throws Exception {
		return jobBuilderFactory.get("job")
				.incrementer(new RunIdIncrementer())
				.start(step1)
				.build();
	}
}

@Configuration
class ChunkingConfiguration {

	@Bean
	@ServiceActivator(outputChannel = MC_REPLIES_OUT, inputChannel = MC_REPLIES_IN)
	RemoteChunkHandlerFactoryBean chunkHandler(TaskletStep step1,
	                                           ChunkMessageChannelItemWriter<Customer> chunkWriter) throws Exception {
		RemoteChunkHandlerFactoryBean<Customer> factoryBean = new RemoteChunkHandlerFactoryBean<>();
		factoryBean.setChunkWriter(chunkWriter);
		factoryBean.setStep(step1);
		return factoryBean;
	}

	@Bean
	ChunkMessageChannelItemWriter<Customer> chunkWriter(MessagingTemplate template,
                                @Qualifier(MC_REQUESTS_IN) PollableChannel requestsIn) {
		ChunkMessageChannelItemWriter<Customer> chunkWriter = new ChunkMessageChannelItemWriter<>();
		chunkWriter.setMessagingOperations(template);
		chunkWriter.setReplyChannel(requestsIn);
		chunkWriter.setMaxWaitTimeouts(10);
		return chunkWriter;
	}

	@Bean
	MessagingTemplate messageTemplate(@Qualifier(MC_REQUESTS_OUT) MessageChannel requestsOut) {
		MessagingTemplate messagingTemplate = new MessagingTemplate();
		messagingTemplate.setDefaultChannel(requestsOut);
		return messagingTemplate;
	}

	@Bean(name = MC_REPLIES_IN)
	MessageChannel repliesIn() {
		return MessageChannels.direct().get();
	}

	@Bean(name = MC_REPLIES_OUT)
	MessageChannel repliesOut() {
		return MessageChannels.queue().get();
	}

	@Bean(name = MC_REQUESTS_OUT)
	MessageChannel requestsOut() {
		return MessageChannels.queue().get();
	}

	@Bean(name = MC_REQUESTS_IN)
	PollableChannel requestsIn() {
		return MessageChannels.queue().get();
	}
}

/*

@Configuration
class IntegrationConfiguration {

	@Bean
	  ChunkMessageChannelItemWriter<?> chunkWriter() {
		ChunkMessageChannelItemWriter<?> chunkWriter = new ChunkMessageChannelItemWriter<>();
		chunkWriter.setMessagingOperations(messageTemplate());
		chunkWriter.setReplyChannel(inboundReplies());
		chunkWriter.setMaxWaitTimeouts(10);

		return chunkWriter;
	}

	@Bean
	@ServiceActivator(inputChannel = "inboundRequests", outputChannel = "outboundReplies")
	  RemoteChunkHandlerFactoryBean chunkHandler(TaskletStep step1) throws Exception {
		RemoteChunkHandlerFactoryBean factoryBean = new RemoteChunkHandlerFactoryBean();

		factoryBean.setChunkWriter(chunkWriter());
		factoryBean.setStep(step1);

		return factoryBean;
	}

	@Bean
	  MessagingTemplate messageTemplate() {
		MessagingTemplate messagingTemplate = new MessagingTemplate(requestsOut());
		messagingTemplate.setReceiveTimeout(60_000_000);
		return messagingTemplate;
	}

	@Bean
	  QueueChannel inboundReplies() {
		return MessageChannels.queue().get();
	}

	@Bean
	  QueueChannel outboundReplies() {
		return MessageChannels.queue().get();
	}

	@Bean
	  Queue amqpRequestsQueue() {
		return new Queue("chunking.requests", false);
	}

	@Bean
	  MessageChannel requestsOut() {
		return MessageChannels.direct().get();
	}

	@Bean
	  PollableChannel inboundRequests() {
		return new QueueChannel();
	}

	@Bean
	@ServiceActivator(inputChannel = "requestsOut")
	  AmqpOutboundEndpoint amqpOutboundEndpoint(AmqpTemplate template) {
		AmqpOutboundEndpoint endpoint = new AmqpOutboundEndpoint(template);
		endpoint.setExpectReply(true);
		endpoint.setOutputChannel(inboundReplies());
		endpoint.setRoutingKey("chunking.requests");
		return endpoint;
	}

	@Bean
	  AmqpInboundGateway inbound(SimpleMessageListenerContainer listenerContainer) {
		AmqpInboundGateway gateway = new AmqpInboundGateway(listenerContainer);

		gateway.setRequestChannel(inboundRequests());
		gateway.setRequestTimeout(60000000l);
		gateway.setReplyChannel(outboundReplies());


		return gateway;
	}


	@Bean(name = PollerMetadata.DEFAULT_POLLER)
	  PollerMetadata defaultPoller() {
		PollerMetadata pollerMetadata = new PollerMetadata();
		pollerMetadata.setTrigger(new PeriodicTrigger(1000));
		return pollerMetadata;
	}

	@Bean
	  SimpleMessageListenerContainer container(ConnectionFactory connectionFactory) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("chunking.requests");
		container.setConcurrentConsumers(4);

		return container;
	}
}
*/
