// TODO: Implement the query b.sql in this file.
use crate::utils::*;

extern crate wake;
use polars::prelude::{DataFrame, NamedFrom};
use polars::series::{ChunkCompare, Series};
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

/// This node implements the SQL query provided above.

pub fn query(
    table_input_map: HashMap<String, TableInput>,
    result_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    
    // Map storing table names and their associated columns.
    let tables_to_columns_map = HashMap::from([
        ("orders".into(), vec!["o_custkey", "o_totalprice"]),
        ("customer".into(), vec!["c_name", "c_custkey", "c_mktsegment"]),
    ]);

    // Creating CSVReaderNodes for tables.
    let orders_reader_node = build_csv_reader_node("orders".into(), &table_input_map, &tables_to_columns_map);
    let customer_reader_node = build_csv_reader_node("customer".into(), &table_input_map, &tables_to_columns_map);

    // Node for 'WHERE' condition.
    let filter_automobile_customers_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|dataframe: &DataFrame| {
            let market_segment_column = dataframe.column("c_mktsegment").unwrap();
            let mask = market_segment_column.equal("AUTOMOBILE").unwrap();
            dataframe.filter(&mask).unwrap()
        })))
        .build();

    // Node for 'HASH JOIN' operation.
    let orders_customer_join_node = HashJoinBuilder::new()
        .left_on(vec!["o_custkey".into()])
        .right_on(vec!["c_custkey".into()])
        .build();

    // Node for 'GROUP BY AGGREGATE' operation.
    let mut total_price_accumulator = SumAccumulator::new();
    total_price_accumulator
        .set_group_key(vec!["c_name".to_string()])
        .set_aggregates(vec![
            ("o_totalprice".into(), vec!["sum".into()]),
        ]);

    let group_by_customer_name_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(total_price_accumulator)
        .build();

    // Node for 'SELECT' operation.
    let select_and_sort_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|dataframe: &DataFrame| {
            let columns = vec![
                Series::new("c_name", dataframe.column("c_name").unwrap()),
                Series::new("o_totalprice_sum", dataframe.column("o_totalprice_sum").unwrap()),
            ];
            DataFrame::new(columns)
                .unwrap()
                .sort(&["o_totalprice_sum"], vec![true])
                .unwrap()
        })))
        .build();

    // Connecting the nodes.
    filter_automobile_customers_node.subscribe_to_node(&customer_reader_node, 0);
    orders_customer_join_node.subscribe_to_node(&orders_reader_node, 0); // Left Node
    orders_customer_join_node.subscribe_to_node(&filter_automobile_customers_node, 1); // Right Node
    group_by_customer_name_node.subscribe_to_node(&orders_customer_join_node, 0);
    select_and_sort_node.subscribe_to_node(&group_by_customer_name_node, 0);

    // Subscribing the result reader to the output node.
    result_reader.subscribe_to_node(&select_and_sort_node, 0);

    // Adding all nodes to the service for execution.
    let mut execution_service = ExecutionService::<polars::prelude::DataFrame>::create();
    execution_service.add(customer_reader_node);
    execution_service.add(orders_reader_node);
    execution_service.add(filter_automobile_customers_node);
    execution_service.add(orders_customer_join_node);
    execution_service.add(group_by_customer_name_node);
    execution_service.add(select_and_sort_node);
    
    execution_service
}
