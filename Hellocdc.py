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

    return base_table, load_frequency, partition_details

def update_yaml(file_path, base_table, load_frequency, partition_details):
    lines = load_yaml_lines(file_path)
    updated = False
    new_lines = []
    i = 0
    insert_index = None
    inside_s4_block = False

    while i < len(lines):
        line = lines[i]


        if '{% if sql_flavour.upper() == \'S4\' %}' in line:
            inside_s4_block = True
            insert_index = i + 1


        match = re.match(r'^\s*#?\s*- base_table:\s*' + re.escape(base_table) + r'\s*$', line)
        if match:

            new_lines.append(f"- base_table: {base_table}\n")
            i += 1
            if i < len(lines) and 'load_frequency' in lines[i]:
                new_lines.append(f"  load_frequency: \"{load_frequency}\"\n")
                i += 1
            else:
                new_lines.append(f"  load_frequency: \"{load_frequency}\"\n")

            if partition_details:
                if i < len(lines) and 'partition_details' in lines[i]:
                    i += 1
                new_lines.append("  partition_details: {\n")
                new_lines.append(f"    column: \"{partition_details['column']}\", partition_type: \"{partition_details['partition_type']}\", time_grain: \"{partition_details['time_grain']}\"\n")
                new_lines.append("  }\n")
            updated = True
        elif '{% endif %}' in line and inside_s4_block and not updated:

            new_lines.append(f"  - base_table: {base_table}\n")
            new_lines.append(f"    load_frequency: \"{load_frequency}\"\n")
            if partition_details:
                new_lines.append("    partition_details: {\n")
                new_lines.append(f"      column: \"{partition_details['column']}\", partition_type: \"{partition_details['partition_type']}\", time_grain: \"{partition_details['time_grain']}\"\n")
                new_lines.append("    }\n")
            updated = True
            new_lines.append(line)
            i += 1
        else:
            new_lines.append(line)
            i += 1

    if not updated:

        new_lines.append(f"- base_table: {base_table}\n")
        new_lines.append(f"  load_frequency: \"{load_frequency}\"\n")
        if partition_details:
            new_lines.append("  partition_details: {\n")
            new_lines.append(f"    column: \"{partition_details['column']}\", partition_type: \"{partition_details['partition_type']}\", time_grain: \"{partition_details['time_grain']}\"\n")
            new_lines.append("  }\n")

    write_yaml_lines(file_path, new_lines)
    print(f"✅ YAML file updated successfully for table: {base_table}")

# 🔧 Run the script
if __name__ == "__main__":
    print("🚀 Starting CDC YAML updater...")
    yaml_file = "cdc_settings.yaml"
    base_table, load_frequency, partition_details = prompt_user()
    update_yaml(yaml_file, base_table, load_frequency, partition_details)