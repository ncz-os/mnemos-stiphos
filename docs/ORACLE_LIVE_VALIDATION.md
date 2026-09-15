# Oracle claim validation

The September 15, 2026 review exercised the canonical `stiphos-oracle/2` DDL
against Oracle 26ai (23.26.1) with python-oracledb thin mode. The 1,000-job,
eight-worker HTTP claim test exposed two driver integration failures: decoded
JSON lists were passed back to `json.loads`, and description CLOB locators
escaped the repository into HTTP serialization. Both now become ordinary Python
values while the connection is still open. Unit coverage includes materialized
JSON, text/LOB JSON, subscription pools, and descriptions longer than 4,000 bytes.

For live testing, provision a **dedicated empty schema**, apply
`mnemos/hive_mind/oracle_ddl.sql`, and supply its connection URL as `ORACLE_DSN`.
Install core and KNEMON test dependencies plus this package's `test,oracle,mcp`
extras. Then run:

```sh
python -m pytest tests/test_concurrency_parity.py tests/test_oracle_repository_complete.py -q
```

The fixture validates the schema version and cleans only its generated project
from canonical `JOBS`. Do not point it at a production queue: claiming is global
for eligible worker kinds. The Db2 parity parameter remains explicitly skipped
because a Db2 STIPHOS repository does not exist. Core's Db2 memory backend is a
separate capability. These checks establish uniqueness and completeness of this
claim workload, not sustained fleet throughput or every Oracle service method.
