In this section, we create the paths to source files, the paths to archive files, definitions for the schemas, and definitions for log and data tables used in the projects.

We use SQL for all schemas, volumes and table definitions.
Databricks dbtulil was use to create source and archive file paths in the volume
These are listed below

```
---Create schema 'Bronze' only if schema with same name doesn't exist
CREATE SCHEMA IF NOT EXISTS Bronze;


---Create schema 'Silver' only if schema with same name doesn't exist
CREATE SCHEMA IF NOT EXISTS Silver;

---Create schema 'Gold' only if schema with same name doesn't exist
CREATE SCHEMA IF NOT EXISTS Gold;

---Create schema 'Logs' only if schema with same name doesn't exist
CREATE SCHEMA IF NOT EXISTS Log;

```

Create volume

```
-- Create a managed volume with full schema name
CREATE VOLUME Workspace.Bronze.Raw_files
```

```

#Create landing directories for files

#customers
dbutils.fs.mkdirs("/Volumes/workspace/bronze/raw_files/customers/landing")
dbutils.fs.mkdirs("/Volumes/workspace/bronze/raw_files/customers/archive")

#orders
dbutils.fs.mkdirs("/Volumes/workspace/bronze/raw_files/orders/landing")
dbutils.fs.mkdirs("/Volumes/workspace/bronze/raw_files/orders/archive")

#orders
dbutils.fs.mkdirs("/Volumes/workspace/bronze/raw_files/products/landing")
dbutils.fs.mkdirs("/Volumes/workspace/bronze/raw_files/products/archive")

#orders
dbutils.fs.mkdirs("/Volumes/workspace/bronze/raw_files/suppliers/landing")
dbutils.fs.mkdirs("/Volumes/workspace/bronze/raw_files/suppliers/archive")
```

Create Log Table

```
-- Create an empty Delta table with a defined schema

CREATE TABLE Log.table_logs (
    Schema String,
    Table_name STRING,
    Status STRING,
    Created_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (`Schema`, `Table_name`)
COMMENT 'Table to store log information';

```

Create bronze customer table

```
create table if not exists bronze.customers (
  CustomerID int,
  CustomerFirstName string,
  CustomerLastName string,
  PhoneNumber string,
  FaxNumber string,
  Email string,
  Address string,
  City string,
  State string,
  Zip string,
  Country string,
  ValidFrom timestamp,
  ValidTo timestamp,
  insert_dt timestamp,
  record_creation_date timestamp
)
USING DELTA
COMMENT 'Table to store customer information';

```

Create silver customers table

```
create table if not exists silver.customers (
  CustomerID int,
  CustomerFirstName string,
  CustomerLastName string,
  PhoneNumber string,
  FaxNumber string,
  Email string,
  Address string,
  City string,
  State string,
  Zip string,
  Country string,
  insert_dt timestamp,
  effective_start_dt timestamp,
  effective_end_dt timestamp,
  active_flg int,
  update_dt timestamp,
  record_creation_date timestamp
)
USING DELTA
PARTITIONED BY (`Country`)
COMMENT 'Table to store customer information';

```

Create gold customer table

```
create table if not exists gold.customers (
  skey string,
  CustomerID int,
  CustomerFirstName string,
  CustomerLastName string,
  PhoneNumber string,
  FaxNumber string,
  Email string,
  Address string,
  City string,
  State string,
  Zip string,
  Country string,
  active_flg int,
  insert_dt timestamp,
  update_dt timestamp,
  record_creation_date timestamp,
  effective_start_dt timestamp,
  effective_end_dt timestamp,
  hash1 string,
  hash2 string
)
USING DELTA
PARTITIONED BY (`Country`)
COMMENT 'Table to store customer information';

```

Create bronze product table

```
create table if not exists bronze.products (
  ProductID int,
  ProductName string,
  Color string,
  Brand string,
  Size string,
  Barcode string,
  TaxRate float,
  UnitPrice float ,
  SupplierId int,
  ValidFrom timestamp,
  ValidTo timestamp,
  insert_dt timestamp,
  record_creation_date timestamp
)
USING DELTA
PARTITIONED BY (`SupplierId`)
COMMENT 'Table to store product information';
```

Create Bronze supplier table

```
create table if not exists bronze.suppliers (
  SupplierId int,
  SupplierName string,
  PhoneNumber string,
  Email string,
  ValidFrom timestamp,
  ValidTo timestamp,
  insert_dt timestamp,
  record_creation_date timestamp
)
USING DELTA
PARTITIONED BY (`SupplierId`)
COMMENT 'Table to store supplier information';
```

Create bronze orders table

```
create table if not exists bronze.orders (
  OrderID int,
  ProductID int ,
  CustomerID int,
  Quantity int,
  UnitPrice float(10,2),
  TaxRate float(10,2),
  LastEditedBy int,
  LastEditedWhen timestamp,
  LastEditedDate timestamp,
  insert_dt timestamp,
  record_creation_date timestamp
)
USING DELTA
PARTITIONED BY (`CustomerID`)
COMMENT 'Table to store order information';
```

Create silver products table

```
create table if not exists silver.products (
  ProductID int,
  ProductName string,
  Color string,
  Brand string,
  Size string,
  Barcode string,
  TaxRate float,
  UnitPrice float ,
  SupplierId int,
  active_flg int,
  insert_dt timestamp,
  update_dt timestamp,
  record_creation_date timestamp
)
USING DELTA
PARTITIONED BY (`SupplierId`)
COMMENT 'Table to store product information';
```

Create silver suppliers table

```
create table if not exists silver.suppliers (
  SupplierId int,
  SupplierName string,
  PhoneNumber string,
  Email string,
  insert_dt timestamp,
  effective_start_dt timestamp,
  effective_end_dt timestamp,
  active_flg int,
  update_dt timestamp,
  record_creation_date timestamp
)
USING DELTA
PARTITIONED BY (`SupplierId`)
COMMENT 'Table to store supplier information';
```

create silver orders table

```
create table if not exists silver.orders (
  OrderID int,
  ProductID int ,
  CustomerID int,
  Quantity int,
  UnitPrice float(10,2),
  TaxRate float(10,2),
  LastEditedBy int,
  LastEditedWhen timestamp,
  LastEditedDate timestamp,
  insert_dt timestamp,
  effective_start_dt timestamp,
  effective_end_dt timestamp
)
using delta
PARTITIONED BY (`OrderID`)
```

Create gold products table

```
create table if not exists gold.products(
  skey string,
  ProductID int,
  ProductName string,
  Color string,
  Brand string,
  Size string,
  Barcode string,
  TaxRate float,
  UnitPrice float ,
  SupplierId int,
  insert_dt timestamp,
  update_dt timestamp,
  record_creation_date timestamp,
  active_flg int,
  effective_start_dt timestamp,
  effective_end_dt timestamp,
  hash1 string,
  hash2 string
)
USING DELTA
PARTITIONED BY (`SupplierId`)
COMMENT 'Table to store product information';
```

Create gold suppliers table

```
create table if not exists gold.suppliers(
  skey string,
  SupplierID int,
  SupplierName string,
  PhoneNumber string,
  Email string,
  insert_dt timestamp,
  update_dt timestamp,
  record_creation_date timestamp,
  active_flg int,
  effective_start_dt timestamp,
  effective_end_dt timestamp,
  hash1 string,
  hash2 string
)
USING DELTA
PARTITIONED BY (`SupplierID`)
COMMENT 'Table to store supplier information';
```

Create gold orders_fact table

```
create table if not exists gold.orders_fact(
  date_skey string,
  product_skey string,
  supplier_skey string,
  customer_skey string,
  OrderID int,
  ProductID int ,
  CustomerID int,
  Quantity int,
  UnitPrice float(10,2),
  TaxRate float(10,2),
  LastEditedBy int,
  lastEditedWhen timestamp,
  LastEditedDate timestamp,
  effective_start_dt timestamp,
  effective_end_dt timestamp,
  active_flg int,
  insert_dt timestamp,
  update_dt timestamp,
  record_creation_date timestamp

)
USING DELTA
PARTITIONED BY (`OrderID`,`active_flg`)
COMMENT 'Table to store order_facts information';
```
