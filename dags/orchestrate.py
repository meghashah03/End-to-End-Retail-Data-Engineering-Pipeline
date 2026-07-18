import time
from databricks.sdk import WorkspaceClient
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import pendulum

@dag(
    dag_id="orchestrate",
    schedule="0 11 * * *",
    catchup=False,
    start_date=pendulum.datetime(year=2026, month=7, day=20, tz="America/Halifax"),
)

def orchestrate():

    @task
    def ingest_cdc():
        ws= WorkspaceClient(
                host="databricks_host",    #hidden for security reasons
                token="databricks_token"   #hidden
            )

        job_trigger=ws.jobs.run_now(job_id="databricks_job_id")  #hidden

        while True:

            job_run=ws.jobs.get_run(job_trigger.run_id)
            
            print(f"Job run state: {job_run.state.life_cycle_state}, result state: {job_run.state.result_state}")

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state==RunResultState.SUCCESS:
                    print("Job completed successfully")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
                    
            time.sleep(5) #wait for 5 seconds before checking the job status again

    
    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        #Manually set the working directory using cd command
        return "cd /opt/airflow/walmart_project && dbt source freshness --profiles-dir ."
    

    silver_technical= BashOperator(
        task_id='silver_technical',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select silver_t'
    )

    silver_technical_tests= BashOperator(
        task_id='silver_technical_tests',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt test --select silver_t'
    )

    silver_business= BashOperator(
        task_id='silver_business',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select silver_b'
    )

    silver_business_tests= BashOperator(
        task_id='silver_business_tests',
        bash_command='cd /opt/airflow/walmart_project && dbt test --select silver_b'
    )

    gold_ephemeral= BashOperator(
        task_id='gold_ephemeral',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/ephemeral'
    )

    gold_dimensions= BashOperator(
        task_id='gold_dimensions',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt snapshot'
    )

    gold_facts= BashOperator(
        task_id='gold_facts',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/fact'
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()
