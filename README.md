# Hive serde schema validation
Hive serde for checking data type and showing error if data type is not correct
instead of convert it to NULL which is a default behavior of hive.

## Usage
By default a field delimeter is set to tab but can it can be specified using separatorChar property.
### **Example**

```sql
ADD JAR serde-0.0.1.jar;
CREATE TABLE test
(
	c1 INT,
	c2 STRING
)
ROW FORMAT SERDE 'com.hive.serde.Schemavalidation'
WITH serdeproperties ("separatorChar" = ",");
```
