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

# 清理所有失败的任务实例
def clear_failed_tasks(dag_id):
    try:
        print(f"正在清理 DAG 的失败任务实例: {dag_id}")
        clear_command = ["airflow", "tasks", "clear", "-y", "--only-failed", dag_id]
        subprocess.run(clear_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error while clearing failed tasks for DAG {dag_id}: {e}")

# 获取所有 DAG ID
dag_ids = get_dag_ids()

# 遍历每个 DAG ID，清理失败的任务实例
for dag_id in dag_ids:
    clear_failed_tasks(dag_id)

print("所有 DAG 的失败任务实例已被清理。")
