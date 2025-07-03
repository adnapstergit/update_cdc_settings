import yaml
import re

def load_yaml_lines(file_path):
    with open(file_path, 'r') as f:
        return f.readlines()

def write_yaml_lines(file_path, lines):
    with open(file_path, 'w') as f:
        f.writelines(lines)

def prompt_user():
    table_type = input("Is this a partitioned table? (yes/no): ").strip().lower()
    base_table = input("Enter the base_table name: ").strip()
    load_frequency = input("Enter the load_frequency (e.g., '@daily' or '0/15 * * * *'): ").strip()

    partition_details = None
    if table_type == 'yes':
        column = input("Enter the partition column name (e.g., 'recordstamp'): ").strip()
        partition_type = input("Enter the partition type (e.g., 'time'): ").strip()
        time_grain = input("Enter the time grain (e.g., 'day'): ").strip()
        partition_details = {
            'column': column,
            'partition_type': partition_type,
            'time_grain': time_grain
        }

    cluster_details = None
    cluster_input = input("Do you want to add cluster_details? (yes/NA): ").strip().lower()
    if cluster_input == 'yes':
        columns_input = input("Enter cluster columns as comma-separated values (e.g., rldnr,bttype,vrgng): ").strip()
        columns = [col.strip() for col in columns_input.split(',') if col.strip()]
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

def update_yaml(file_path, base_table, load_frequency, partition_details, cluster_details):
    lines = load_yaml_lines(file_path)
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

    if updated:
        write_yaml_lines(file_path, new_lines)
        print(f"✅ YAML file updated successfully for table: {base_table}")
    else:
        print(f"⚠️ Could not update or insert table: {base_table}")


if __name__ == "__main__":
    print("🚀 Starting CDC YAML updater...")
    yaml_file = "cdc_settings.yaml"
    base_table, load_frequency, partition_details, cluster_details = prompt_user()
    update_yaml(yaml_file, base_table, load_frequency, partition_details, cluster_details)
