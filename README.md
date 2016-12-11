# Hive serde schema validation
Hive serde for checking data type and display error if data type is not correct.
Instead of convert it to NULL which is a default behavior of hive.

## Usage
By default, a field delimeter is set to tab but it can be specified using **separatorChar** property.
It can check correctness of primitive hive data type. For **BOOLEAN** data type, it can convert value 0 to false and 1 to true.

### **Example**

```sql
ADD JAR serde-0.0.1.jar;
CREATE TABLE test
(
	c1 INT,
	c2 STRING
)
ROW FORMAT SERDE 'com.hive.serde.Schemavalidation'
WITH serdeproperties ('separatorChar' = ',');
```

### Build
```shell
mvn clean install
```
