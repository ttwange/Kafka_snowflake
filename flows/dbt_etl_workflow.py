from prefect import task, flow
from dbt.cli.main import dbtRunner


@task(log_prints=True,retries=3)
def run_dbt():
    dbt_runner = dbtRunner()
    dbt_runner.run()

@flow(name="dbt_workflow")
def main():
    dbt_task = run_dbt()
    dbt_project_path = "./dbt_transformation"
    return dbt_task

if __name__ == "__main__":
    main()
