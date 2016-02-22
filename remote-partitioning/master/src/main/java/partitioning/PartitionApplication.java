package partitioning;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.integration.partition.MessageChannelPartitionHandler;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchDatabaseInitializer;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.annotation.*;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StopWatch;

import javax.sql.DataSource;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static partitioning.PartitionApplication.STEP1;
import static partitioning.PartitionApplication.WORKER_STEP;
import static partitioning.PartitionMaster.MASTER_REPLIES;
import static partitioning.PartitionMaster.MASTER_REPLIES_AGGREGATED;

@EnableBatchProcessing
@IntegrationComponentScan
@SpringBootApplication
@EnableBinding(PartitionMaster.class)
class PartitionApplication {

	public static final String STEP1 = "step1";
	public static final String WORKER_STEP = "workerStep";

	public static void main(String args[]) {
		SpringApplication.run(PartitionApplication.class, args);
	}
}

@Component
class JobDurationListener implements JobExecutionListener {

	private Log log = LogFactory.getLog(getClass());

	private StopWatch stopWatch;

	@Override
	public void beforeJob(JobExecution jobExecution) {
		stopWatch = new StopWatch();
		stopWatch.start("Processing image submissions");
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		stopWatch.stop();

		long duration = stopWatch.getLastTaskTimeMillis();

		log.info(String.format("Job took: %d minutes, %d seconds.",
				MILLISECONDS.toMinutes(duration), MILLISECONDS.toSeconds(duration) - MINUTES.toSeconds(MILLISECONDS.toMinutes(duration))));
	}
}

@Component
class ColumnRangePartitioner implements Partitioner {

	private final JdbcOperations jdbcTemplate;

	private final String table;

	private final String column;

	@Autowired
	ColumnRangePartitioner(JdbcOperations jdbcTemplate,
	                       @Value("${partition.table:customer}") String table,
	                       @Value("${partition.column:id}") String column) {
		this.jdbcTemplate = jdbcTemplate;
		this.table = table;
		this.column = column;
	}

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		int min = jdbcTemplate.queryForObject("SELECT MIN(" + column + ") from " + table, Integer.class);
		int max = jdbcTemplate.queryForObject("SELECT MAX(" + column + ") from " + table, Integer.class);
		int targetSize = (max - min) / gridSize + 1;

		Map<String, ExecutionContext> result = new HashMap<String, ExecutionContext>();
		int number = 0;
		int start = min;
		int end = start + targetSize - 1;

		while (start <= max) {
			ExecutionContext value = new ExecutionContext();
			result.put("partition" + number, value);

			if (end >= max) {
				end = max;
			}
			value.putInt("minValue", start);
			value.putInt("maxValue", end);
			start += targetSize;
			end += targetSize;
			number++;
		}

		return result;
	}
}

@Configuration
class PartitionMasterChannels {

	@Autowired
	private PartitionMaster partitionMaster;

	@Bean(name = MASTER_REPLIES_AGGREGATED)
	QueueChannel masterRequestsAggregated() {
		return MessageChannels.queue().get();
	}

	DirectChannel masterRequests() {
		return partitionMaster.masterRequests();
	}

	DirectChannel masterReplies() {
		return partitionMaster.masterReplies();
	}

}

interface PartitionMaster {

	String MASTER_REPLIES = "masterReplies";

	String MASTER_REQUESTS = "masterRequests";

	String MASTER_REPLIES_AGGREGATED = "masterRepliesAggregated";

	@Output(MASTER_REQUESTS)
	DirectChannel masterRequests();

	@Input(MASTER_REPLIES)
	DirectChannel masterReplies();

}

@Configuration
class PartitionedJobConfiguration {

	private Log log = LogFactory.getLog(getClass());

	@Value("${partition.grid-size:4} ")
	private int gridSize = 4;


	@Bean
	MessagingTemplate messageTemplate(PartitionMasterChannels master) {
		MessagingTemplate messagingTemplate = new MessagingTemplate(master.masterRequests());
		messagingTemplate.setReceiveTimeout(60 * 1000 * 60);
		return messagingTemplate;
	}

	@MessageEndpoint
	public static class AggregatorMessagingEndpoint {

		private final MessageChannelPartitionHandler partitionHandler;

		@Autowired
		public AggregatorMessagingEndpoint(MessageChannelPartitionHandler partitionHandler) {
			this.partitionHandler = partitionHandler;
		}

		@Aggregator(
				inputChannel = MASTER_REPLIES,
				outputChannel = MASTER_REPLIES_AGGREGATED,
				sendTimeout = "3600000",
				sendPartialResultsOnExpiry = "true")
		public List<?> aggregate(@Payloads List<?> messages) {
			return this.partitionHandler.aggregate(messages);
		}
	}


	@Bean
	MessageChannelPartitionHandler partitionHandler(MessagingTemplate messagingTemplate,
	                                                JobExplorer jobExplorer,
	                                                PartitionMasterChannels master) throws Exception {
		MessageChannelPartitionHandler partitionHandler = new MessageChannelPartitionHandler();
		partitionHandler.setStepName(WORKER_STEP);
		partitionHandler.setGridSize(this.gridSize);
		partitionHandler.setReplyChannel(master.masterRequestsAggregated());
		partitionHandler.setMessagingOperations(messagingTemplate);
		partitionHandler.setPollInterval(5000L);
		partitionHandler.setJobExplorer(jobExplorer);
		return partitionHandler;
	}

	@Bean
	@StepScope
	JdbcPagingItemReader<Customer> pagingItemReader(
			DataSource dataSource,
			@Value("#{stepExecutionContext['minValue']}") Long minValue,
			@Value("#{stepExecutionContext['maxValue']}") Long maxValue) {


		log.info("reading " + minValue + " to " + maxValue);

		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, firstName, lastName, birthdate");
		queryProvider.setFromClause("from customer");
		queryProvider.setWhereClause("where id >= " + minValue + " and id <= " + maxValue);
		queryProvider.setSortKeys(Collections.singletonMap("id", Order.ASCENDING));

		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();
		reader.setDataSource(dataSource);
		reader.setFetchSize(1000);
		reader.setQueryProvider(queryProvider);
		reader.setRowMapper((rs, i) -> new Customer(rs.getLong("id"), rs.getString("firstName"), rs.getString("lastName"), rs.getDate("birthdate")));
		return reader;
	}

	@Bean
	@StepScope
	JdbcBatchItemWriter<Customer> customerItemWriter(DataSource dataSource) {
		JdbcBatchItemWriter<Customer> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		itemWriter.setSql("INSERT INTO NEW_CUSTOMER VALUES (:id, :firstName, :lastName, :birthdate)");
		itemWriter.setDataSource(dataSource);
		return itemWriter;
	}

	@Bean(name = STEP1)
	Step step1(StepBuilderFactory stepBuilderFactory,
	           ColumnRangePartitioner partitioner,
	           PartitionHandler partitionHandler,
	           @Qualifier(WORKER_STEP) Step step) throws Exception {
		return stepBuilderFactory.get(STEP1)
				.partitioner(step.getName(), partitioner)
				.step(step)
				.partitionHandler(partitionHandler)
				.build();
	}

	@Bean
	ColumnRangePartitioner partitioner(JdbcOperations template) {
		return new ColumnRangePartitioner(template, "customer".toUpperCase(), "id".toUpperCase());
	}

	@Bean(name = WORKER_STEP)
	Step workerStep(StepBuilderFactory stepBuilderFactory) {
		return stepBuilderFactory.get(WORKER_STEP)
				.<Customer, Customer>chunk(1000)
				.reader(pagingItemReader(null, null, null))
				.writer(customerItemWriter(null))
				.build();
	}

	@Bean(name = PollerMetadata.DEFAULT_POLLER)
	PollerMetadata defaultPoller() {
		PollerMetadata pollerMetadata = new PollerMetadata();
		pollerMetadata.setTrigger(new PeriodicTrigger(10));
		return pollerMetadata;
	}

}


@Component
class MyBatchDatabaseInitializer extends BatchDatabaseInitializer {

	private final TransactionTemplate transactionTemplate;
	private final JdbcTemplate jdbcTemplate;

	private String tables =
			" BATCH_JOB_EXECUTION          " +
					"| BATCH_JOB_EXECUTION_CONTEXT  " +
					"| BATCH_JOB_EXECUTION_PARAMS   " +
					"| BATCH_JOB_EXECUTION_SEQ      " +
					"| BATCH_JOB_INSTANCE           " +
					"| BATCH_JOB_SEQ                " +
					"| BATCH_STEP_EXECUTION         " +
					"| BATCH_STEP_EXECUTION_CONTEXT " +
					"| BATCH_STEP_EXECUTION_SEQ     ";

	@Autowired
	public MyBatchDatabaseInitializer(TransactionTemplate transactionTemplate, JdbcTemplate jdbcTemplate) {
		this.transactionTemplate = transactionTemplate;
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	protected void initialize() {
		this.transactionTemplate.execute(tx -> {
			List<String> tables = Arrays.asList(this.tables.split("\\|")).stream()
					.map(String::trim)
					.filter(x -> !x.equals(""))
					.collect(Collectors.toList());

			jdbcTemplate.execute("SET foreign_key_checks = 0;");

			tables.forEach(t -> jdbcTemplate.execute("drop table " + t + ";"));

			jdbcTemplate.execute("SET foreign_key_checks = 1;");

			return null;
		});

		super.initialize();
	}
}

@Configuration
@Profile("master")
class MasterConfiguration {

	@Bean
	Job job(JobBuilderFactory jobBuilderFactory, @Qualifier(STEP1) Step step) throws Exception {
		return jobBuilderFactory.get("job")
				.incrementer(new RunIdIncrementer())
				.start(step)
				.build();
	}
}
/*

@Configuration
@Profile("worker")
class WorkerConfiguration {

	@Bean
	StepLocator stepLocator() {
		return new BeanFactoryStepLocator();
	}

	@Bean
	AmqpInboundGateway inbound(SimpleMessageListenerContainer listenerContainer) {
		AmqpInboundGateway gateway = new AmqpInboundGateway(listenerContainer);
		gateway.setRequestChannel(inboundRequests());
		gateway.setRequestTimeout(60_000_000L);
		return gateway;
	}

	@Bean
	@ServiceActivator(inputChannel = "inboundRequests", outputChannel = "outboundStaging")
	StepExecutionRequestHandler stepExecutionRequestHandler(JobExplorer explorer, StepLocator stepLocator) {
		StepExecutionRequestHandler stepExecutionRequestHandler = new StepExecutionRequestHandler();
		stepExecutionRequestHandler.setStepLocator(stepLocator);
		stepExecutionRequestHandler.setJobExplorer(explorer);
		return stepExecutionRequestHandler;
	}

}
*/



/*
	*/




/*

	@Bean
	@ServiceActivator(inputChannel = "outboundRequests")
	AmqpOutboundEndpoint amqpOutboundEndpoint(AmqpTemplate template) {
		AmqpOutboundEndpoint endpoint = new AmqpOutboundEndpoint(template);
		endpoint.setExpectReply(true);
		endpoint.setOutputChannel(inboundRequests());
		endpoint.setRoutingKey("partition.requests");
		return endpoint;
	}
*/


/*
	@Bean
	SimpleMessageListenerContainer container(ConnectionFactory connectionFactory) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("partition.requests");
		container.setConcurrentConsumers(1);

		return container;
	}*/

/*
@Bean
	DirectChannel outboundRequests() {
		return MessageChannels.direct().get();
	}
	@Bean
	QueueChannel outboundStaging() {
		return MessageChannels.queue().get();
	}

	@Bean
	QueueChannel inboundRequests() {
		return MessageChannels.queue().get();
	}
*/


class Customer {

	private final long id;

	private final String firstName;

	private final String lastName;

	private final Date birthdate;

	public Customer(long id, String firstName, String lastName, Date birthdate) {
		this.id = id;
		this.firstName = firstName;
		this.lastName = lastName;
		this.birthdate = birthdate;
	}

	public long getId() {
		return id;
	}

	public String getFirstName() {
		return firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public Date getBirthdate() {
		return birthdate;
	}

	@Override
	public String toString() {
		return "Customer{" +
				"id=" + id +
				", firstName='" + firstName + '\'' +
				", lastName='" + lastName + '\'' +
				", birthdate=" + birthdate +
				'}';
	}
}