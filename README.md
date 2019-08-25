# spanner_pk_sample

query throughput of Cloud Spanner.

- 1 node in 1 region
- all query are executed in one transaction.

![QueryThrouput](https://user-images.githubusercontent.com/301822/63657505-dfec0e80-c7dd-11e9-87ce-96457ed850c2.png)


## Example (Cloud Functions)

### environment variable

| env | content |
----|----
| GCP_PROJECT | GCP project ID |
| SPN_INSTANCE_ID | instance ID of Cloud Spanner |
| SPN_DATABASE_ID | database in instance  |

### event of Cloud Functions

```
{
  "mode":"random_num_timestamp",
  "testmode":"batch",
  "num":10,
  "delete":true
}
```

## Example (OS)

### environment variable

| env | content |
----|----
| SPN_PROJECT_ID | GCP project ID |
| SPN_INSTANCE_ID | instance ID of Cloud Spanner |
| SPN_DATABASE_ID | database in instance  |

