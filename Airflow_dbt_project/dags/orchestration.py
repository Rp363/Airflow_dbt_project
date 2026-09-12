from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState, RunlifeCycleState
import time, pendulum

@dag(
    dag_id="orchestration_dag",
    schedule="0 11 * * *",
    catchup=False,
    start_date= pendulum.datetime(2026, 9, 12, tz="UTC")
)

def orchestration():
    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host="https://host.cloud.databricks.com",
            token = '12345'
       )

        job_trigger = ws.jobs.run_now(
            job_id = 12345
        )

        while True:
            job_status = ws.jobs.get_run(run_id=job_trigger.run_id)
            # print(f"Job status: {job_status.state.life_cycle_state}, Result state: {job_status.state.result_state}")

            if job_status.state.life_cycle_state in [RunlifeCycleState.TERMINATED, RunlifeCycleState.SKIPPED, RunlifeCycleState.INTERNAL_ERROR]:
                if job_status.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    break
            else:
                print(f"Job failed with result state: {job_status.state.result_state}")
                break

            time.sleep(5)

    @task
    def cleanup():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        # manually set the working directory to the dbt project directory
        return "cd /opt/airflow/walmart_project && dbt source freshness"

        # source_freshness = BashOperator(
        #     cwd="/opt/airflow/walmart_project",
        #     bash_command="dbt source freshness"
        # )

    silver_tech = BashOperator(
        task_id="silver_tech",
        bash_command="cd /opt/Airflow_dbt_project/walmart_project && dbt run --select silver_t"
    )

    silver_tech_test = BashOperator(
        task_id="silver_tech_test",
        bash_command="cd /opt/Airflow_dbt_project/walmart_project && dbt test --select silver_t"
    )

    silver_business = BashOperator(
        task_id="silver_business",
        bash_command="cd /opt/Airflow_dbt_project/walmart_project && dbt test --select silver_b"
    )

    gold_ephemeral = BashOperator(
        task_id = 'gold_ephemeral',
        cwd = "/opt/Airflow_dbt_project/walmart_project",
        bash_command = "dbt run --select gold/ephemeral"
    )

    gold_dim = BashOperator(
        task_id = 'gold_dim',
        cwd = "/opt/Airflow_dbt_project/walmart_project",
        bash_command = "dbt snapshot"
    )

    gold_fact = BashOperator(
        task_id = 'gold_fact',
        cwd = "/opt/Airflow_dbt_project/walmart_project",
        bash_command = "dbt run --select gold/fact"
    )


    ingest_cdc() >> cleanup() >> source_freshness() >> silver_tech >> silver_tech_test >> silver_business >> gold_ephemeral >> gold_dim >> gold_fact

orchestration_dag = orchestration()