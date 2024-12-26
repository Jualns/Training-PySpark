# High-Value Customer Identification

## Description
You need to process a customer dataset to identify high-value customers. Specifically, you will:
1. Read data from a CSV file.
2. Filter out customers with a purchase amount less than 100 USD.
3. Further filter to include only customers aged 30 or above.
4. Use `display(df)` to show the final DataFrame.

---

## Input

### File Path
`/datasets/customers.csv`

### Schema
| Column          | Type    |
|------------------|---------|
| customer_id      | integer |
| name             | string  |
| email            | string  |
| age              | integer |
| purchase_amount  | float   |

### Example Data
| customer_id | name             | email              | age | purchase_amount |
|-------------|------------------|--------------------|-----|-----------------|
| 1           | Alice Johnson    | alice@email.com    | 25  | 150.50          |
| 2           | Bob Smith        | bob@email.com      | 32  | 200.00          |
| 3           | Charlie Brown    | charlie@email.com  | 29  | 75.00           |
| 4           | Diana Prince     | diana@email.com    | 40  | 120.00          |
| 5           | Evan Davis       | evan@email.com     | 35  | 90.00           |

---

## Output

### Call `display` Function
Use `display(df)` to show the final DataFrame.

### Schema
| Column         | Type    |
|-----------------|---------|
| customer_id     | integer |
| name            | string  |
| purchase_amount | float   |

### Example Data
| customer_id | name        | purchase_amount |
|-------------|-------------|-----------------|
| 2           | Bob Smith   | 200.00          |
| 4           | Diana Prince| 120.00          |

---

## Explanation
The output DataFrame (`df_result`) includes customers who:
1. Have a purchase amount of at least 100 USD.
2. Are aged 30 or above.

In the example:
- **Bob Smith** and **Diana Prince** meet these criteria, so they are included in the result.

## Solutions

- [Using SQL]()
- [Using PySpark]()