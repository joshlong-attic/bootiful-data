package partition;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.batch.BatchDatabaseInitializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
class MySqlBatchDatabaseInitializer extends BatchDatabaseInitializer {

	private boolean resetBatch = true;

	@Autowired
	private TransactionTemplate transactionTemplate;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private final static String TABLES[] =
			(" BATCH_JOB_EXECUTION | BATCH_JOB_EXECUTION_CONTEXT | BATCH_JOB_EXECUTION_PARAMS |" +
					" BATCH_JOB_EXECUTION_SEQ | BATCH_JOB_INSTANCE | BATCH_JOB_SEQ | BATCH_STEP_EXECUTION |" +
					" BATCH_STEP_EXECUTION_CONTEXT | BATCH_STEP_EXECUTION_SEQ")
					.trim()
					.split("\\|");

	@Override
	protected void initialize() {
		if (this.resetBatch) {
			this.transactionTemplate.execute(tx -> {
				List<String> tables =
						Arrays.asList(TABLES)
								.stream()
								.map(String::trim)
								.filter(x -> !x.equals(""))
								.collect(Collectors.toList());
				jdbcTemplate.execute("SET foreign_key_checks = 0;");
				tables.forEach(t -> jdbcTemplate.execute("drop table " + t + ";"));
				jdbcTemplate.execute("SET foreign_key_checks = 1;");
				return null;
			});
		}
		super.initialize();
	}
}