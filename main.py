from dbt.cli.main import dbtRunner, dbtRunnerResult

# initialize
dbt = dbtRunner()

# create CLI args as a list of strings
# cli_args = ["run", "--select", "tag:my_tag"]

cli_args = ["build"]

# run the command
res: dbtRunnerResult = dbt.invoke(cli_args)

# inspect the results
for r in res.result:
    print(f"{r.node.name}: {r.status}")
