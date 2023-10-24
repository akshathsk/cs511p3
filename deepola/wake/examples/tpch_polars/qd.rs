use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

pub fn query(
    input_tables: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    
    // Mapping of table names to their columns used in the query.
    let columns_by_table = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_partkey", "l_extendedprice", "l_discount", "l_shipinstruct", "l_quantity"],
        ),
        ("part".into(), vec!["p_partkey", "p_brand", "p_size"]),
    ]);

    let lineitem_reader_node = build_csv_reader_node("lineitem".into(), &input_tables, &columns_by_table);
    let part_reader_node = build_csv_reader_node("part".into(), &input_tables, &columns_by_table);

    let ship_instruction_filter_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let ship_instruction = df.column("l_shipinstruct").unwrap();
            let mask = ship_instruction.equal("DELIVER IN PERSON").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    let join_on_partkey_node = HashJoinBuilder::new()
        .left_on(vec!["p_partkey".into()])
        .right_on(vec!["l_partkey".into()])
        .build();

    let brand_and_quantity_filter_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let brand = df.column("p_brand").unwrap();
            let quantity = df.column("l_quantity").unwrap();
            let size = df.column("p_size").unwrap();
            let mask1 = brand.equal("Brand#12").unwrap()
                & size.gt_eq(1i32).unwrap()
                & size.lt_eq(5i32).unwrap()
                & quantity.gt_eq(1i32).unwrap()
                & quantity.lt_eq(11i32).unwrap();
            let mask2 = brand.equal("Brand#23").unwrap()
                & size.gt_eq(1i32).unwrap()
                & size.lt_eq(10i32).unwrap()
                & quantity.gt_eq(10i32).unwrap()
                & quantity.lt_eq(20i32).unwrap();
            let mask3 = brand.equal("Brand#34").unwrap()
                & size.gt_eq(1i32).unwrap()
                & size.lt_eq(15i32).unwrap()
                & quantity.gt_eq(20i32).unwrap()
                & quantity.lt_eq(30i32).unwrap();

            let combined_mask = mask1 | mask2 | mask3;
            df.filter(&combined_mask).unwrap()
        })))
        .build();

    let calculate_discounted_price_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap().clone();
            let discount = df.column("l_discount").unwrap().clone();
            let columns = vec![
                Series::new(
                    "disc_price",
                    extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64),
                ),
            ];
            df.hstack(&columns).unwrap()
        })))
        .build();

    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator.set_aggregates(vec![("disc_price".into(), vec!["sum".into()])]);
    
    let sum_disc_price_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    let select_revenue_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let columns = vec![Series::new("revenue", df.column("disc_price").unwrap())];
            DataFrame::new(columns).unwrap()
        })))
        .build();

    ship_instruction_filter_node.subscribe_to_node(&lineitem_reader_node, 0);
    join_on_partkey_node.subscribe_to_node(&part_reader_node, 0);
    join_on_partkey_node.subscribe_to_node(&ship_instruction_filter_node, 1);
    brand_and_quantity_filter_node.subscribe_to_node(&join_on_partkey_node, 0);
    calculate_discounted_price_node.subscribe_to_node(&brand_and_quantity_filter_node, 0);
    sum_disc_price_node.subscribe_to_node(&calculate_discounted_price_node, 0);
    select_revenue_node.subscribe_to_node(&sum_disc_price_node, 0);

    output_reader.subscribe_to_node(&select_revenue_node, 0);

    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_reader_node);
    service.add(part_reader_node);
    service.add(ship_instruction_filter_node);
    service.add(join_on_partkey_node);
    service.add(brand_and_quantity_filter_node);
    service.add(calculate_discounted_price_node);
    service.add(sum_disc_price_node);
    service.add(select_revenue_node);
    service
}
