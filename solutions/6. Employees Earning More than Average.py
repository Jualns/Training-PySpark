from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame
from StartSparkSession import start_spark
from pyspark.sql import functions

def SQL_solution(spark: SparkSession, Tipo: str):

    if Tipo == "subquery_corr":
        # Solução utilizando subquery correlacionada, onde calculamos a média AVG(e2.salary) dependente do id do departamento e.departament_id
        # Menos eficiente em grandes datasets pois a subquery é recalculada para cada linha da base
        query = """
        SELECT 
            e.employee_name,
            d.department_name, 
            e.salary
        FROM 
            employee e
        LEFT JOIN 
            department d
        ON 
            e.department_id = d.department_id
        WHERE 
            e.salary > (
                SELECT 
                    AVG(e2.salary)
                FROM 
                    employee e2
                WHERE 
                    e2.department_id = e.department_id
            );
        """
    elif Tipo == "subquery_decorr":
        # Solução utilizando subquery não correlacionada, calculando uma única vez a média salarial dos departamentos
        # Calculando a média salarial e posteriormente e fazendo o JOIN dos funcionários
        query = """
        SELECT 
            e.employee_name,
            d.department_name,
            e.salary
        FROM 
            employee e
        LEFT JOIN 
            department d
        ON 
            e.department_id = d.department_id
        JOIN (
            SELECT 
                department_id,
                AVG(salary) AS avg_salary
            FROM 
                employee
            GROUP BY 
                department_id
        ) das
        ON 
            e.department_id = das.department_id
        WHERE 
            e.salary > das.avg_salary;
        """
    elif Tipo == "CTE":
        # Solução utilizando Common Table Expression, calculando a média salarial dos departamentos uma única vez
        # Mais eficiente, facilmente legível e podendo ser reutilizada pois está separada no DepartamentAvgSalary

        query = """
        WITH DepartmentAvgSalary AS (
            SELECT 
                department_id,
                AVG(salary) AS avg_salary
            FROM 
                employee
            GROUP BY 
                department_id
        )
        SELECT 
            e.employee_name,
            d.department_name,
            e.salary
        FROM 
            employee e
        LEFT JOIN 
            department d
        ON 
            e.department_id = d.department_id
        JOIN 
            DepartmentAvgSalary das
        ON 
            e.department_id = das.department_id
        WHERE 
            e.salary > das.avg_salary;
        """
    elif Tipo == "Having":
        # Utilizando agregação e having
        # Dependendo da situação pode ser mais eficiente, depende da curva de distribução dos dados, nesse caso a curva salarial pois 
        #  essa solução faz as contas após a agregação
        query = """
        SELECT 
            e.employee_name,
            d.department_name,
            e.salary
        FROM 
            employee e
        LEFT JOIN 
            department d
        ON 
            e.department_id = d.department_id
        GROUP BY 
            e.employee_name, d.department_name, e.salary, e.department_id
        HAVING 
            e.salary > (
                SELECT 
                    AVG(salary)
                FROM 
                    employee e2
                WHERE 
                    e2.department_id = e.department_id
            );
        """
    else:
        # não utilizado o else para ter mais clareza nos tipos de solução
        return None

    # Using SQL to solve the problem
    selected_df = spark.sql(query)

    return selected_df

def PySpark_solution(df: DataFrame):
    "Solution based on pyspark without SQL"
    result_df = df.withColumn(
        "final_price",
        col("original_price") * (1 - col("discount_percentage") / 100)
    )

    return result_df.select(col("product_id"), col("product_name"), col("final_price"))

# Initialize Spark session
spark = start_spark()

# Employee DataFrame
employee_data = [
    (1, "Alice", 5000, 1),
    (2, "Bob", 7000, 2),
    (3, "Charlie", 4000, 1),
    (4, "David", 6000, 2),
    (5, "Eve", 8000, 3),
    (6, "Kev", 9000, 3),
    (7, "Mev", 10000, 3),
    (8, "Mob", 12000, 2)
]
employee_columns = ["employee_id", "employee_name", "salary", "department_id"]
emp_df = spark.createDataFrame(employee_data, employee_columns)

# Department DataFrame
department_data = [
    (1, "HR"),
    (2, "Engineering"),
    (3, "Finance")
]
department_columns = ["department_id", "department_name"]
dept_df = spark.createDataFrame(department_data, department_columns)

emp_df.createOrReplaceTempView("employee")
dept_df.createOrReplaceTempView("department")

try:
    sql_solution_types = ("subquery_corr", "subquery_decorr", "CTE", "Having")

    for type_sol in sql_solution_types:
        print(f"\nTipo de Solução SQL - {type_sol}")
        selected_df = SQL_solution(spark, type_sol)

        # Display the final DataFrame
        selected_df.show()

except Exception as err:
    print(f"Erro: {err}")
finally:
    spark.stop()