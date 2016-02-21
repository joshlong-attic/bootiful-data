package com.example;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcOperations;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class ColumnRangePartitionerTest {


	JdbcOperations jdbcOperations;

	@Before
	public void setUp() throws Exception {
		this.jdbcOperations = Mockito.mock(JdbcOperations.class);
	}

	@Test
	public void testColumnPartitioner() throws Exception {
		SimplePartitioner crp = new SimplePartitioner(this.jdbcOperations, "CUSTOMERS", "ID");

		// int min = jdbcTemplate.queryForObject("SELECT MIN(" + column + ") from " + table, Integer.class); /
		Mockito.when(this.jdbcOperations.queryForObject("SELECT MIN(ID) from CUSTOMERS", Integer.class)).thenReturn(1);
		Mockito.when(this.jdbcOperations.queryForObject("SELECT MAX(ID) from CUSTOMERS", Integer.class)).thenReturn(20_000);

		Map<String, ExecutionContext> partition = crp.partition(20);
		assertNotNull(partition);
		partition.entrySet().forEach(e -> System.out.println(e.getKey() + '=' + e.getValue()));


	}
}