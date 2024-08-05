import requests
import json
import os


def get_all_serialized_dags(airflow_base_url, auth=None, output_dir='serialized_dags'):
    dags_url = f"{airflow_base_url}/api/v1/dags"
    dag_files = []

    try:
        os.makedirs(output_dir, exist_ok=True)
        response = requests.get(dags_url, auth=auth)
        response.raise_for_status()
        dags_list = response.json().get('dags', [])

        for dag in dags_list:
            dag_id = dag['dag_id']
            dag_details_url = f"{airflow_base_url}/api/v1/dags/{dag_id}/details"
            response = requests.get(dag_details_url, auth=auth)
            response.raise_for_status()
            dag_details = response.json()
            dag_file_path = os.path.join(output_dir, f"{dag_id}.json")
            with open(dag_file_path, 'w') as f:
                json.dump(dag_details, f, indent=4)
            dag_files.append(dag_file_path)

        print(f"Serialized DAGs have been saved to the '{output_dir}' directory.")
        return dag_files

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return []


def get_dag_tasks(airflow_base_url, dag_id, auth=None):
    tasks_url = f"{airflow_base_url}/api/v1/dags/{dag_id}/tasks"
    try:
        response = requests.get(tasks_url, auth=auth)
        response.raise_for_status()
        tasks = response.json().get('tasks', [])
        if not tasks:
            print(f"Warning: No tasks found for DAG {dag_id}")
        return tasks
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving tasks for DAG {dag_id}: {e}")
        return []


def normalize_tasks(tasks):
    for task in tasks:
        task['downstream_task_ids'] = sorted(task.get('downstream_task_ids', []))
    return sorted(tasks, key=lambda x: x['task_id'])


def create_dependency_graph(tasks):
    graph = {}
    for task in tasks:
        graph[task['task_id']] = task.get('downstream_task_ids', [])

    dependency_graph = []
    for task_id, downstream_tasks in graph.items():
        if downstream_tasks:
            dependency_graph.append(f"{task_id} >> {' >> '.join(downstream_tasks)}")
        else:
            dependency_graph.append(f"{task_id} (no downstream tasks)")

    return dependency_graph


def generate_dependency_graphs(airflow_base_url, auth=None, output_dir='serialized_dags',
                               graph_output_dir='dependency_graphs'):
    dag_files = get_all_serialized_dags(airflow_base_url, auth, output_dir)
    try:
        os.makedirs(graph_output_dir, exist_ok=True)
        dag_graphs = {}
        for dag_file in dag_files:
            with open(dag_file, 'r') as f:
                serialized_dag = json.load(f)
            dag_id = serialized_dag['dag_id']
            tasks = get_dag_tasks(airflow_base_url, dag_id, auth)
            if not tasks:
                print(f"Warning: No tasks found for DAG {dag_id}")
                continue
            tasks = normalize_tasks(tasks)
            dependency_graph = create_dependency_graph(tasks)
            if not dependency_graph:
                print(f"Warning: No dependency graph created for DAG {dag_id}")
                continue
            graph_file_path = os.path.join(graph_output_dir, f"{dag_id}_dependency_graph.txt")
            with open(graph_file_path, 'w') as f:
                for line in dependency_graph:
                    f.write(line + '\n')
            dag_graphs[dag_id] = dependency_graph
        print(f"Dependency graphs have been saved to the '{graph_output_dir}' directory.")
        return dag_graphs
    except Exception as e:
        print(f"Error: {e}")
        return {}


def compare_dag_graphs(graphs1, graphs2):
    report = {
        'unique_to_env1': [],
        'unique_to_env2': [],
        'different_graphs': []
    }

    dags1 = set(graphs1.keys())
    dags2 = set(graphs2.keys())

    unique_to_env1 = dags1 - dags2
    unique_to_env2 = dags2 - dags1
    common_dags = dags1 & dags2

    for dag in unique_to_env1:
        report['unique_to_env1'].append(dag)

    for dag in unique_to_env2:
        report['unique_to_env2'].append(dag)

    for dag in common_dags:
        if graphs1[dag] != graphs2[dag]:
            print(f"Differences found in DAG {dag}:")
            print("Env1 graph:", graphs1[dag])
            print("Env2 graph:", graphs2[dag])
            report['different_graphs'].append(dag)

    return report


def write_report(report, file_path='dag_comparison_report.txt'):
    with open(file_path, 'w') as f:
        f.write("DAGs unique to environment 1:\n")
        for dag in report['unique_to_env1']:
            f.write(f"- {dag}\n")

        f.write("\nDAGs unique to environment 2:\n")
        for dag in report['unique_to_env2']:
            f.write(f"- {dag}\n")

        f.write("\nDAGs with different dependency graphs:\n")
        for dag in report['different_graphs']:
            f.write(f"- {dag}\n")

    print(f"Report has been written to {file_path}")


def main():
    airflow_base_url_env1 = 'http://localhost:8080'  # Replace with your Airflow instance 1 base URL
    auth_env1 = ('admin', 'admin')  # Replace with your Airflow instance 1 credentials
    airflow_base_url_env2 = 'http://localhost:8081'  # Replace with your Airflow instance 2 base URL
    auth_env2 = ('admin', 'admin')  # Replace with your Airflow instance 2 credentials

    graphs_env1 = generate_dependency_graphs(airflow_base_url_env1, auth_env1, output_dir='serialized_dags_env1',
                                             graph_output_dir='dependency_graphs_env1')
    graphs_env2 = generate_dependency_graphs(airflow_base_url_env2, auth_env2, output_dir='serialized_dags_env2',
                                             graph_output_dir='dependency_graphs_env2')

    report = compare_dag_graphs(graphs_env1, graphs_env2)
    write_report(report)


if __name__ == "__main__":
    main()