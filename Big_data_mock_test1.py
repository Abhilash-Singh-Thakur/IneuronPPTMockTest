# 1. Write a PySpark code to read a CSV file named "employees.csv" containing the following columns: "employee_id", "name", "age", "department". Display the top 10 records from the DataFrame.

Solution --

from pyspark.sql import SparkSession


def main():
    # initiate the spark session object.
    spark = SparkSession.builder.master("yarn").appName("StudentETL").getOrCreate()

    # Load data from employee.csv file 
    employee_DF = spark.read.csv("/input_data/employee.csv", header=True, inferSchema=True) 

    # Showing the 10 records from the dataframe
    employee_DF.show(10) 
    
    # spark session stopped
    spark.stop() 


# Driver code
if __name__ == '__main__':
    
    main()





# 2.  Given a PySpark DataFrame named "sales_data" with columns "product_name" and "revenue", write a code to calculate the total revenue for each product and display the result in descending order.

# Solution ---
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

def main():
    # create the spark session
    spark = SparkSession.builder.master("yarn").appName("SalesETL").getOrCreate()

    # load the csv file and create the dataframe.
    sales_data = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

    # grouped the record by product name and aggregate the sum of revenue of each product group
    sales_revenue_df = sales_data.groupBy("product_name").agg(sum("revenue").alias("total_revenue"))

    # create the new dataframe in descending order
    result_df = sales_revenue_df.orderBy("total_revenue",ascending=False)

    # Show all the results
    result_df.show()
    
    # At the end stop the spark session.
    spark.stop()

# Driver Code 
if __name__=='__main__':
    main()



# 3. Write a PySpark code to read a JSON file named "students.json" containing student records with the following schema: "name" (string), "age" (integer), "grade" (string). Filter the DataFrame to include only students whose age is greater than 18.

# Solution--

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

def main():
    # create the spark session.
    spark = SparkSession.builder.master("yarn").appName("studentETL").getOrCreate()

    # Define the schema 
    schema = StructType([\
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("grade", IntegerType(), True)
    ])
    
    # Load the json file.
    student_df = spark.read.json("students.json", schema=schema)

    # filter the record where age > 18
    student_filter_df = student_df.filter(student_df.age>18)

    # show all the students whose age is greater then 18
    student_filter.show()
    
    # spark session stopped
    spark.stop()

# Driver code
if __name__=='__main__':
    main()




# 4. Consider a PySpark DataFrame named "transactions" with columns "transaction_id", "user_id", and "amount". Write a code to calculate the average transaction amount for each user and display the result.

# Solution ---

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def main():
    # initiate the sparksession object
    spark = SparkSession.builder.master("yarn").appName("transactionETL").getOrCreate() 
    # load the transactions.csv file into the dataframe
    transactions = spark.read.csv("transactions.csv",header=True, inferSchema=True)
    # create new data frame with group by user id and aggregate the amount of transaction of each user.
    average_amount_df = transactions.groupBy("user_id").agg(avg("amount").alias("average_amount"))
    # Show all the records with average amount.
    average_amount_df.show()

# Driver code.
if __name__=='__main__':
    main()




# 5. Given a PySpark DataFrame named "logs" with columns "timestamp" (timestamp) and "event" (string), write a code to count the number of events that occurred in each hour and display the result sorted by the hour.

Soution ---

from pyspark.sql import SparkSession
from pyspark.sql.functions import hour



def main():
    # create the spark session object 
    spark = SparkSession.builder.master("yarn").appName("logsETL").getOrCreate()
    # load the data from logs.csv file
    logs = spark.read.csv("logs.csv",inferSchema=True, header=True)
    # Extract hour from timestamp column
    logs_with_hour_df = logs.withColumn("hour", hour(logs.timestamp))
    # group by hour and count it and orderby hour
    event_count_df = logs_with_hour_df.groupBy("hour").count().orderBy("hour")
    # Show all the records from the dataframe.
    event_count_df.show()
    
    spark.stop()

if __name__ == '__main__':
    main()



# 6.  Retrieve all the customers from the "Customers" table whose age is greater than 25 and have made at least one purchase.

# Solution - 
SELECT * FROM Customer WHERE age>25 AND purchase > 0;

# 7. Find the total number of orders placed by each customer and display the results in descending order of the number of orders.

# Solution -
SELECT customer_id, customer_name, sum(orders)as total_orders  from customer group by customer_id, customer_name order by total_orders;

# 8. Retrieve the names of all products that are currently out of stock from the "Products" table.

#Soution -
SELECT  names FROM products WHERE stock_quantity = 0;

# 9. Calculate the average price of all products in each category and display the results along with the category name.

#Soution -
SELECT category_name, AVG(price) AS average_price FROM Products GROUP BY category_name;

# 10. Retrieve the top 5 customers who have spent the highest total amount on purchases.

#Soution -
SELECT customer_name, SUM(total_amount) AS total_spent FROM Purchases GROUP BY customer_name ORDER BY total_spent DESC LIMIT 5; 
