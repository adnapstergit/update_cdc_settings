import os
import re
import requests

def load_yaml_lines_from_github(raw_url):
    response = requests.get(raw_url)
    response.raise_for_status()
    return response.text.splitlines(keepends=True)

def write_yaml_lines(file_path, lines):
    with open(file_path, 'w') as f:
        f.writelines(lines)

def get_inputs_from_env():
    base_table = os.environ.get("BASE_TABLE", "").strip()
    load_frequency = os.environ.get("LOAD_FREQUENCY", "").strip()

    partition_details = None
    if os.environ.get("IS_PARTITIONED", "no").strip().lower() == "yes":
        partition_details = {
            'column': os.environ.get("PARTITION_COLUMN", "").strip(),
            'partition_type': os.environ.get("PARTITION_TYPE", "").strip(),
            'time_grain': os.environ.get("TIME_GRAIN", "").strip()
        }

    cluster_details = None
    if os.environ.get("ADD_CLUSTER", "NA").strip().lower() == "yes":
        columns = [col.strip() for col in os.environ.get("CLUSTER_COLUMNS", "").split(",") if col.strip()]
        cluster_details = {'columns': columns}

    return base_table, load_frequency, partition_details, cluster_details

def format_entry(indent, base_table, load_frequency, partition_details, cluster_details):
    lines = [f"{indent}- base_table: {base_table}\n"]
    lines.append(f"{indent}  load_frequency: \"{load_frequency}\"\n")
    if partition_details:
        lines.append(f"{indent}  partition_details: {{\n")
        lines.append(f"{indent}    column: \"{partition_details['column']}\", partition_type: \"{partition_details['partition_type']}\", time_grain: \"{partition_details['time_grain']}\"\n")
        lines.append(f"{indent}  }}\n")
    if cluster_details:
        lines.append(f"{indent}  cluster_details: {{columns: {cluster_details['columns']}}}\n")
    return lines

def update_yaml(lines, base_table, load_frequency, partition_details, cluster_details):
    new_lines = []
    inside_s4 = False
    inside_ecc = False
    indent = ""
    updated = False

    non_partitioned_index = None
    partitioned_index = None
    table_found = False
    i = 0

    while i < len(lines):
        line = lines[i]

        if '{% if sql_flavour.upper() == \'ECC\' %}' in line:
            inside_ecc = True
            indent = ""
        elif '{% if sql_flavour.upper() == \'S4\' %}' in line:
            inside_s4 = True
            indent = "  "

        if inside_ecc:
            if '# PARTITIONED TABLES' in line:
                partitioned_index = i
            elif re.match(r'^\s*#?\s*- base_table:', line) and non_partitioned_index is None:
                non_partitioned_index = i

        match = re.match(r'^\s*#?\s*- base_table:\s*' + re.escape(base_table) + r'\s*$', line)
        if match:
            table_found = True
            new_lines.append(line)
            i += 1
            while i < len(lines) and re.match(r'^\s{2,}\S', lines[i]):
                i += 1
            new_lines = new_lines[:-1]
            new_lines.extend(format_entry(indent, base_table, load_frequency, partition_details, cluster_details))
            updated = True
            continue

        new_lines.append(line)
        i += 1

    if not table_found:
        if partition_details:
            target_index = partitioned_index + 1 if partitioned_index is not None else len(new_lines)
        else:
            target_index = non_partitioned_index if non_partitioned_index is not None else len(new_lines)
        entry_lines = format_entry(indent, base_table, load_frequency, partition_details, cluster_details)
        new_lines = new_lines[:target_index] + entry_lines + new_lines[target_index:]
        updated = True

    return new_lines, updated

if __name__ == "__main__":
    print("🚀 Starting CDC YAML updater...")

    # Replace with your actual raw GitHub URL
    github_raw_url = "https://raw.githubusercontent.com/adnapstergit/update_cdc_settings/main/cdc_settings.yaml"
    local_output_file = "updated_cdc_settings.yaml"

    try:
        yaml_lines = load_yaml_lines_from_github(github_raw_url)
    except Exception as e:
        print(f"❌ Failed to fetch YAML from GitHub: {e}")
        exit(1)

    base_table, load_frequency, partition_details, cluster_details = get_inputs_from_env()
    updated_lines, was_updated = update_yaml(yaml_lines, base_table, load_frequency, partition_details, cluster_details)

    if was_updated:
        write_yaml_lines(local_output_file, updated_lines)
        print(f"✅ YAML file updated and saved to: {local_output_file}")
    else:
        print(f"⚠️ No changes made to the YAML file.")
