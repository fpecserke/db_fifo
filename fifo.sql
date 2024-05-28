/*
    # Generalisation for FIFO models from inputs and outputs
    ---
    This is generalized solution to matching any kind of input table with the same output table to create a 'in_out' fact table based on their order of operations

    General terms:
    - 'qty_in' - quantity of input (e.g. stock coming into a warehouse)
    - 'qty_out' - quantity of output (e.g. stock leaving a warehouse)
    - 'unique_combination' - a unique information that is shared between inputs and outputs (e.g. combination of warehouse & SKU)
    - 'order_dim' - orderning dimension determining the order in which operations were done (e.g. date or timestamp of operation, or just operation id)


    Prerequisites:
    - inputs can happen any time
    - outputs can only happen if there is something to output (e.g. can't sell something from warehouse if there is nothing on stock)
    - input and output table have the same 'unique_combination' and the combination of 'unique_combination' and 'order_dim' is unique within each table
        - in the warehouse it means both in and out tables have to have the same columns: 'sku' and 'warehouse' 
        - and 1 'sku' in 1 'warehouse' has only 1 'timestamp' and these 3 things can't be the same in another row
        - if the uniqueness prerequisite is not fulfilled, it can be guaranteed by aggregation (sum the qty and make it into 1 row)

    Approach & example (let's take an example of our warehouse):
        - 'input' - stock in 
        - 'output' - stock out 
        - 'unique_combination' - column 'sku'
        - 'order_dim' - timestamp of the in/out

    We want to match inputs that come usually in batches of 10 to outputs which are usually individual.
    So one input can have many outputs.

    The way to match is quite simple:
    1. We make a table with 'cum_sum' (cumulative sum) of all the inputs
    2. If the 'cum_sum' of the outputs is smaller (and bigger than cum_sum of previous batch), we attribute it to that batch 

    That way, if we buy 5 batches of 10 pcs, then everything between 40th and 50th sold piece is sold from the last bought batch.

    But this only works with output quantities of 1. In reality, one input can be sold in many outputs but also one output can be sold from more than one input. 
    To generalize it we can treat outputs also as batches and the matching changes to:
    1. cum_sum of the outputs > cum_sum of previous batch of inputs
    2. cum_sum of the inputs  > cum_sum of previous batch of outputs

    That way, in a more complicated example, if we sell 36th to 45th piece in one go, we can split it in 2 rows: 
    - 5 pieces (36-40) from 4th batch of 10 | 45 (cum_sum_out) > 30 (cum_sum_in previous batch) AND 35 (cum_sum_out previous batch) < 40 (cum_sum_in)
    - 5 pieces (41-45) from last batch of 10 | 45 (cum_sum_out) > 40 (cum_sum_in previous batch) AND 35 (cum_sum_out previous batch) < 50 (cum_sum_in)

    Note: We have matched the output into 2 rows based on this condition, but the number of pieces that are assigned to each input batch have to be calculated as a last step
*/

with inputs as ( 
select 
    unique_combination,
    order_dim, -- usually date
    qty_in,
    sum(qty_in) over (partition by unique_combination order by order_dim asc) as qty_in_cum, -- cumsum
    sum(qty_in) over (partition by unique_combination order by order_dim asc) - qty_in as qty_in_cum_pb -- cumsum of previous batch
from inputs 
),
outputs as (
select 
    unique_combination,
    order_dim, -- usually date
    qty_out,
    sum(qty_out) over (partition by unique_combination order by order_dim asc) as qty_out_cum, -- cumsum
    sum(qty_out) over (partition by unique_combination order by order_dim asc) - qty_out as qty_out_cum_pb -- cumsum of previous batch
from outputs
),
big_join as (
select 
    unique_combination, 
    i.order_dim as in_order_dim,
    o.order_dim as out_order_dim,
    i.qty_in_cum,
    -- whether the "out" operation was finished on this row, or does it continue on next row
    iif(i.qty_in_cum < o.qty_out_cum, 'Unfinished out', 'Finished out') as out_status, 
    /* 
        Here we calculate this to get the cum_sum of handled pieces (to uncumulate it into how much was actually sold from this batch in the next step).
        It is the smaller of the 2 values (i.qty_in_cum, o.qty_out_cum), here's why in the example of the 2 rows:
        - ROW 1 | 45 (qty_out_cum) > 30 (qty_in_cum_pb) AND 35 (qty_out_cum_pb) < 40 (qty_in_cum) | smaller is 40 (qty_in_cum), we handled 40pcs, we still need next row
        - ROW 2 | 45 (qty_out_cum) > 40 (qty_in_cum_pb) AND 35 (qty_out_cum_pb) < 50 (qty_in_cum) | smaller is 45 (qty_out_cum), we handled all 45pcs, this output is finished 
    */
    iif(i.qty_in_cum < o.qty_out_cum, i.qty_in_cum, o.qty_out_cum) as cum_qty -- total cumulative sum of handled pieces
FROM inputs as i
left join outputs as o on 
    i.unique_combination = o.unique_combination
    and o.qty_out_cum   > i.qty_in_cum_pb 
    and i.qty_in_cum    > o.qty_out_cum_pb
)
select 
    unique_combination, 
    in_order_dim,
    out_order_dim,
    out_status,
    qty_in_cum - min_cum as qty_left_from_this_batch, --analytical, not needed here
    /*
        Here we just subtract the min_cum from the previous value and in the context of the 2 rows we get:
        - 40-35 for the first row: 5pcs 
        - 45-50 for the second row: 5pcs
        The pieces are just like we explained in th beginning, but we actually calculated them here in the last step.
    */
    coalesce(lag(min_cum) (PARTITION BY unique_combination order by in_order_dim, out_order_dim asc), 0) - min_cum as qty
from big_join;