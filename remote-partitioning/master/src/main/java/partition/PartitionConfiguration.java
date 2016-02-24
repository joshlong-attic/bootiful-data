package partition;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.integration.partition.MessageChannelPartitionHandler;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Aggregator;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.Payloads;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.scheduling.support.PeriodicTrigger;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.List;


@Configuration
public class PartitionConfiguration {
	public static final String STEP_1 = "step1";

	public static final String WORKER_STEP = "workerStep";


	private Log log = LogFactory.getLog(getClass());

	@Value("${partition.chunk-size}")
	private int chunk;

	@Bean
	MessagingTemplate messageTemplate(PartitionChannels master) {
		MessagingTemplate messagingTemplate = new MessagingTemplate(master.masterRequests());
		messagingTemplate.setReceiveTimeout(60 * 1000 * 60);
		return messagingTemplate;
	}

	@MessageEndpoint
	public static class AggregatorMessagingEndpoint {

		@Autowired
		private MessageChannelPartitionHandler partitionHandler;

		@Aggregator(inputChannel = PartitionChannels.Partition.MASTER_REPLIES,
				outputChannel = PartitionChannels.Partition.MASTER_REPLIES_AGGREGATED,
				sendTimeout = "3600000", sendPartialResultsOnExpiry = "true")
		public List<?> aggregate(@Payloads List<?> messages) {
			return this.partitionHandler.aggregate(messages);
		}
	}

	@Bean
	MessageChannelPartitionHandler partitionHandler(MessagingTemplate messagingTemplate,
	                                                JobExplorer jobExplorer, PartitionChannels master,
	                                                @Value("${partition.grid-size:4}") int gridSize) throws Exception {
		MessageChannelPartitionHandler partitionHandler = new MessageChannelPartitionHandler();
		partitionHandler.setStepName(WORKER_STEP);
		partitionHandler.setGridSize(gridSize);
		partitionHandler.setReplyChannel(master.masterRequestsAggregated());
		partitionHandler.setMessagingOperations(messagingTemplate);
		partitionHandler.setPollInterval(5000L);
		partitionHandler.setJobExplorer(jobExplorer);
		return partitionHandler;
	}

	@Bean
	@StepScope
	JdbcPagingItemReader<Customer> pagingItemReader(DataSource dataSource,
	                                                @Value("#{stepExecutionContext['minValue']}") Long minValue,
	                                                @Value("#{stepExecutionContext['maxValue']}") Long maxValue) {

		log.info("reading " + minValue + " to " + maxValue);

		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, firstName, lastName, birthdate");
		queryProvider.setFromClause("from CUSTOMER");
		queryProvider.setWhereClause("where id >= " + minValue + " and id <= " + maxValue);
		queryProvider.setSortKeys(Collections.singletonMap("id", Order.ASCENDING));

		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();
		reader.setDataSource(dataSource);
		reader.setFetchSize(this.chunk);
		reader.setQueryProvider(queryProvider);
		reader.setRowMapper((rs, i) -> new Customer(rs.getLong("id"), rs.getString("firstName"), rs.getString("lastName"), rs.getDate("birthdate")));
		return reader;
	}

	@Bean
	@StepScope
	JdbcBatchItemWriter<Customer> customerItemWriter(DataSource dataSource) {
		JdbcBatchItemWriter<Customer> writer = new JdbcBatchItemWriter<>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		writer.setSql("INSERT INTO NEW_CUSTOMER VALUES (:id, :firstName, :lastName, :birthdate)");
		writer.setDataSource(dataSource);
		return writer;
	}

	@Bean(name = STEP_1)
	Step step1(StepBuilderFactory stepBuilderFactory,
	           ColumnRangePartitioner partitioner,
	           PartitionHandler partitionHandler,
	           @Qualifier(WORKER_STEP) Step step) throws Exception {
		return stepBuilderFactory
				.get(STEP_1)
				.partitioner(step.getName(), partitioner)
				.step(step)
				.partitionHandler(partitionHandler)
				.build();
	}

	@Bean(name = WORKER_STEP)
	Step workerStep(StepBuilderFactory stepBuilderFactory) {
		return stepBuilderFactory.get(WORKER_STEP)
				.<Customer, Customer>chunk(this.chunk)
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
