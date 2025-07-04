import os
import re
import requests
import yaml 

def load_yaml_lines_from_github(raw_url):
    response = requests.get(raw_url)
    response.raise_for_status()
    return response.text.splitlines(keepends=True)

def write_yaml_lines(file_path, lines):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
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
        lines.append(f"{indent}  partition_details:\n")
        lines.append(f"{indent}    column: \"{partition_details['column']}\"\n")
        lines.append(f"{indent}    partition_type: \"{partition_details['partition_type']}\"\n")
        lines.append(f"{indent}    time_grain: \"{partition_details['time_grain']}\"\n")
    if cluster_details:
        lines.append(f"{indent}  cluster_details:\n")
        lines.append(f"{indent}    columns:\n")
        for col in cluster_details['columns']:
            lines.append(f"{indent}      - \"{col}\"\n")
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
            # Skip old entry
            i += 1
            while i < len(lines) and re.match(r'^\s{2,}\S', lines[i]):
                i += 1
            # Replace with new entry
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

def validate_yaml(lines):
    joined = "".join(lines)
    if "{%" in joined or "{{" in joined:
        print("⚠️ Skipping YAML validation due to Jinja templating.")
        return True
    try:
        yaml.safe_load(joined)
        return True
    except yaml.YAMLError as e:
        print(f"❌ YAML validation error: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Starting CDC YAML updater...")

   
    github_raw_url = "https://raw.githubusercontent.com/adnapstergit/update_cdc_settings/refs/heads/main/cdc_settings.yaml"
    

    try:
        yaml_lines = load_yaml_lines_from_github(github_raw_url)
    except Exception as e:
        print(f"❌ Failed to fetch YAML from GitHub: {e}")
        exit(1)

    base_table, load_frequency, partition_details, cluster_details = get_inputs_from_env()
    updated_lines, was_updated = update_yaml(yaml_lines, base_table, load_frequency, partition_details, cluster_details)

    if was_updated:
        output_path = f"updated_settingsYML/{base_table}_cdc_settings.yaml"
        if validate_yaml(updated_lines):
            write_yaml_lines(output_path, updated_lines)
            print(f"✅ YAML file updated and saved to: {output_path}")
        else:
            print("❌ YAML output is invalid. File not saved.")
    else:
        print(f"⚠️ No changes made to the YAML file.")
