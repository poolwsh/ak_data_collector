import subprocess

# 获取所有 DAG 的 ID
def get_dag_ids():
    try:
        result = subprocess.run(
            ["airflow", "dags", "list"],
            capture_output=True,
            text=True,
            check=True
        )
        dag_ids = [
            line.split()[0].strip()
            for line in result.stdout.split('\n')
            if line.strip() and not line.startswith("dag_id")
        ]
        return dag_ids
    except subprocess.CalledProcessError as e:
        print(f"Error while listing DAGs: {e}")
        return []

# 清理和触发 DAG
def clear_and_trigger_dag(dag_id):
    try:
        print(f"正在清理 DAG 的任务: {dag_id}")
        clear_command = ["airflow", "tasks", "clear", "-y", dag_id]
        subprocess.run(clear_command, check=True)

        print(f"正在触发 DAG: {dag_id}")
        trigger_command = ["airflow", "dags", "trigger", dag_id]
        subprocess.run(trigger_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error while clearing or triggering DAG {dag_id}: {e}")

# 获取所有 DAG ID
dag_ids = get_dag_ids()

# 遍历每个 DAG ID，清理并触发 DAG
for dag_id in dag_ids:
    clear_and_trigger_dag(dag_id)

print("所有 DAG 已被清理并触发。")
