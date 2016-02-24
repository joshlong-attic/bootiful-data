package partition;


import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
class ColumnRangePartitioner implements Partitioner {

	private final JdbcOperations jdbcTemplate;
	private final String table;
	private final String column;

	@Autowired
	ColumnRangePartitioner(JdbcOperations jdbcTemplate,
	                       @Value("${partition.table:CUSTOMER}") String table,
	                       @Value("${partition.column:ID}") String column) {
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
