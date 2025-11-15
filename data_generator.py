# High-performance data generator for Microsoft Fabric/Synapse
# Uses distributed RDD generation with mapPartitions for true parallelism

from pyspark.sql import SparkSession
from pyspark.sql.types import *

def generate_customers(spark, count):
    """Generate customers using distributed RDD mapPartitions."""
    def create_customer_batch(partition):
        from faker import Faker
        import random
        fake = Faker()
        Faker.seed(42)
        
        for idx in partition:
            random.seed(idx + 42)
            fake.seed_instance(idx + 42)
            yield (
                idx,
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                fake.phone_number(),
                fake.street_address(),
                fake.city(),
                fake.state_abbr(),
                fake.zipcode(),
                "USA",
                fake.date_between(start_date="-2y", end_date="today"),
                random.choice([True, False]),
                random.randint(0, 10000)
            )
    
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("address", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("zip_code", StringType()),
        StructField("country", StringType()),
        StructField("registration_date", DateType()),
        StructField("is_active", BooleanType()),
        StructField("loyalty_points", IntegerType()),
    ])
    
    # Use more partitions for better parallelism
    num_partitions = min(200, max(4, count // 5000))
    rdd = spark.sparkContext.parallelize(range(1, count + 1), num_partitions)
    return spark.createDataFrame(rdd.mapPartitions(create_customer_batch), schema)

def generate_products(spark, count):
    """Generate products using distributed RDD mapPartitions."""
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food', 'Beauty']
    
    def create_product_batch(partition):
        from faker import Faker
        import random
        fake = Faker()
        Faker.seed(42)
        
        for idx in partition:
            random.seed(idx + 42)
            fake.seed_instance(idx + 42)
            yield (
                idx,
                fake.catch_phrase(),
                random.choice(categories),
                round(random.uniform(5.99, 999.99), 2),
                round(random.uniform(2.99, 500.00), 2),
                random.randint(0, 1000),
                fake.company(),
                round(random.uniform(1.0, 5.0), 1),
                random.randint(0, 5000),
                random.choice([True, False]),
                fake.date_between(start_date="-3y", end_date="today")
            )
    
    schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("price", DoubleType()),
        StructField("cost", DoubleType()),
        StructField("stock_quantity", IntegerType()),
        StructField("supplier", StringType()),
        StructField("rating", DoubleType()),
        StructField("reviews_count", IntegerType()),
        StructField("is_available", BooleanType()),
        StructField("created_date", DateType()),
    ])
    
    num_partitions = min(200, max(4, count // 5000))
    rdd = spark.sparkContext.parallelize(range(1, count + 1), num_partitions)
    return spark.createDataFrame(rdd.mapPartitions(create_product_batch), schema)



def generate_transactions(spark, count, customers_count, products_count):
    """Generate transactions using distributed RDD mapPartitions."""
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Gift Card']
    statuses = ['Completed', 'Pending', 'Cancelled', 'Refunded']
    discount_options = [0, 5, 10, 15, 20, 25]
    
    def create_transaction_batch(partition):
        from datetime import datetime, timedelta
        import random
        start_date = datetime.now() - timedelta(days=365)
        
        for idx in partition:
            random.seed(idx + 42)
            customer_id = random.randint(1, customers_count)
            product_id = random.randint(1, products_count)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(5.99, 999.99), 2)
            discount_percent = random.choice(discount_options)
            shipping_cost = round(random.uniform(0, 25.00), 2)
            
            transaction_date = start_date + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            subtotal = quantity * unit_price
            discount = subtotal * (discount_percent / 100)
            total_amount = round(subtotal - discount + shipping_cost, 2)
            
            yield (
                idx,
                customer_id,
                product_id,
                quantity,
                unit_price,
                discount_percent,
                transaction_date,
                random.choice(payment_methods),
                shipping_cost,
                random.choice(statuses),
                total_amount
            )
    
    schema = StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("customer_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
        StructField("discount_percent", IntegerType()),
        StructField("transaction_date", TimestampType()),
        StructField("payment_method", StringType()),
        StructField("shipping_cost", DoubleType()),
        StructField("status", StringType()),
        StructField("total_amount", DoubleType()),
    ])
    
    num_partitions = min(200, max(8, count // 5000))
    rdd = spark.sparkContext.parallelize(range(1, count + 1), num_partitions)
    return spark.createDataFrame(rdd.mapPartitions(create_transaction_batch), schema)



def generate_analytics(spark, count):
    """Generate web analytics using distributed RDD mapPartitions."""
    pages = ['/home', '/products', '/cart', '/checkout', '/account', '/support', '/about', '/blog']
    sources = ['Google', 'Facebook', 'Direct', 'Email', 'Twitter', 'Instagram', 'Referral']
    devices = ['Desktop', 'Mobile', 'Tablet']
    browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Opera']
    
    def create_analytics_batch(partition):
        from datetime import datetime, timedelta
        from faker import Faker
        import random
        fake = Faker()
        Faker.seed(42)
        
        for idx in partition:
            random.seed(idx + 42)
            fake.seed_instance(idx + 42)
            yield (
                f'SES{idx:08d}',
                random.randint(1, 2000),
                datetime.now() - timedelta(
                    days=random.randint(0, 90),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                ),
                random.choice(pages),
                random.randint(5, 600),
                random.choice(sources),
                random.choice(devices),
                random.choice(browsers),
                fake.country(),
                random.choice([True, False]),
                random.choice([True, False])
            )
    
    schema = StructType([
        StructField("session_id", StringType(), False),
        StructField("user_id", IntegerType()),
        StructField("visit_time", TimestampType()),
        StructField("page_url", StringType()),
        StructField("time_on_page", IntegerType()),
        StructField("source", StringType()),
        StructField("device", StringType()),
        StructField("browser", StringType()),
        StructField("country", StringType()),
        StructField("bounce", BooleanType()),
        StructField("conversion", BooleanType()),
    ])
    
    num_partitions = min(200, max(8, count // 5000))
    rdd = spark.sparkContext.parallelize(range(1, count + 1), num_partitions)
    return spark.createDataFrame(rdd.mapPartitions(create_analytics_batch), schema)


def get_spark(app_name="DataGenerator", shuffle_partitions=200):
    """Create or retrieve a SparkSession with recommended defaults.

    shuffle_partitions will control `spark.sql.shuffle.partitions` to scale with local test size.
    """

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)
    return spark


def save_table(df, name, base_path=None, format="delta"):
    """Save DataFrame to ABFS path or local table without counting (for performance)."""
    if base_path:
        output_path = f"{base_path.rstrip('/')}/{name}/"
        df.write.mode("overwrite").format(format).save(output_path)
        print(f"✓ Saved to path: {output_path}")
    else:
        df.write.mode("overwrite").format(format).saveAsTable(name)
        print(f"✓ Saved to table: {name}")
    
    return df


def main(scale=1, format="delta", base_path=None):
    """Generate all POC tables using optimized batch processing with Faker.
    
    Performance optimizations:
    - Batch processing (10K rows per batch) reduces memory overhead
    - Pre-generated data avoids UDF serialization overhead
    - Direct DataFrame creation from Python lists
    
    Args:
        scale: Multiplier for data volume (1 = 1K customers, 500 products, 5K transactions, 10K analytics)
        format: Output format ('delta' or 'parquet')
        base_path: Optional ABFS path like 'abfss://container@account.dfs.core.windows.net/path/'
                   If None, saves as managed tables
    
    Example:
        # Save to ABFS
        main(scale=1000, base_path='abfss://mycontainer@mystorage.dfs.core.windows.net/data/')
        
        # Save as managed tables
        main(scale=100)
    """
    spark = get_spark("DataGenerator")
    
    customers_count = 1000 * scale
    products_count = 500 * scale
    transactions_count = 5000 * scale
    analytics_count = 10000 * scale
    
    print(f"Starting data generation with scale={scale}...")
    print(f"  - Customers: {customers_count:,}")
    print(f"  - Products: {products_count:,}")
    print(f"  - Transactions: {transactions_count:,}")
    print(f"  - Analytics: {analytics_count:,}")
    
    # Generate and save tables using batch-optimized approach
    print("\n1/4 Generating customers...")
    df_customers = generate_customers(spark, customers_count)
    save_table(df_customers, "customers", base_path, format)
    
    print("\n2/4 Generating products...")
    df_products = generate_products(spark, products_count)
    save_table(df_products, "products", base_path, format)
    
    print("\n3/4 Generating transactions...")
    df_transactions = generate_transactions(spark, transactions_count, customers_count, products_count)
    save_table(df_transactions, "sales_transactions", base_path, format)
    
    print("\n4/4 Generating analytics...")
    df_analytics = generate_analytics(spark, analytics_count)
    save_table(df_analytics, "web_analytics", base_path, format)
    
    print(f"\n✅ All tables generated successfully with scale={scale}")


if __name__ == "__main__":
    # Example: Generate data at scale 1000 and save to ABFS
    main(
        scale=1000,
        base_path="abfss://stardevseasynapse@stardevseasynapse.dfs.core.windows.net/Testing/Jek/"
    )