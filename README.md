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

---

TODO:
- [x] create basic concept
- [x] write an explanation
- [ ] create an app that lets you map your column and table names and generates an query from the template
- [ ] expand it to include first in last out model (optional)