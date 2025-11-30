# Access Management Example Usage

This guide demonstrates how to use the `AccessManagement` class from the `pysisense` package. Each example below includes a description and a code snippet for easy reference and copying.

---

## Prerequisites

- Ensure `config.yaml` is in the same folder as your script.
- Import required modules and initialize the API client and `AccessManagement` class.

```python
import os
import csv
import json
from pysisense import AccessManagement, SisenseClient

# Set the path to your config file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

# Initialize the API client
api_client = SisenseClient(config_file=config_path, debug=True)

# Initialize the AccessManagement class
access_mgmt = AccessManagement(api_client=api_client)
```

---

## Example 1: Get User Information by Email

Retrieve information for a specific user by their email.

```python
user_email = 'john.doe@example.com'
response = access_mgmt.get_user(user_email)
print(json.dumps(response, indent=4))

# Optional: Convert the response to a DataFrame and print
df = api_client.to_dataframe(response)
print(df)

# Optional: Export the response to a CSV file
api_client.export_to_csv(response, file_name="user_info.csv")
```

---

## Example 2: Get All Users

Fetch all users in the system.

```python
response = access_mgmt.get_users_all()
df = api_client.to_dataframe(response)
print(df)
api_client.export_to_csv(response, file_name="all_users.csv")
```

---

## Example 3: Create a New User (using role and group name)

Create a user by specifying their details, including role and groups.

```python
user_data = {
    "email": "john.doe@example.com",                    # Required: User's email address
    "firstName": "John",                                # Optional: User's first name
    "lastName": "Doe",                                  # Optional: User's last name
    "role": "dataDesigner",                             # Optional: Remove this field if not needed; if omitted, the user will be assigned the default role of 'viewer'. Cannot be an empty string.
    "groups": ["mig_test", "mig_test_2"],               # Optional: List of group names, can be an empty list if the user is not part of any group
    "password": "Sisense141!@",                         # Optional: Provide a password if needed; if omitted, the user will receive an email to set their password. Cannot be an empty string so remove this field if not needed.
    "preferences": {                                    # Optional: User preferences, such as language settings, can be an empty dict if not needed
        "language": "en-US"
    }
}
response = access_mgmt.create_user(user_data)
print(json.dumps(response, indent=4))
```

---

## Example 3b: Create Multiple Users from a CSV File

Bulk create users using a CSV file.

**Expected CSV format:**

```
email,firstName,lastName,role,groups,password,language
john.doe@example.com,John,Doe,dataDesigner,"groupa,groupb","Password123!","en-US"
jane.smith@example.com,Jane,Smith,viewer,"groupa","Password456!","fr-FR"
mike.jones@example.com,Mike,Jones,Designer,"","Password789!","es-ES"
```

**Code:**

```python
csv_file_path = 'new_user.csv'

with open(csv_file_path, mode="r", newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row_num, row in enumerate(reader, start=1):
        if not row.get("email") or not row.get("password"):
            print(f"Skipping row {row_num}: missing email or password")
            continue

        user_data = {
            "email": row["email"],
            "firstName": row.get("firstName", ""),
            "lastName": row.get("lastName", ""),
            "role": row.get("role", "viewer"),
            "groups": [g.strip() for g in row.get("groups", "").split(",") if g.strip()],
            "password": row["password"],
            "preferences": {
                "language": row.get("language", "en-US")
            }
        }

        try:
            response = access_mgmt.create_user(user_data)
            print(json.dumps(response, indent=4))
            if "error" not in response:
                print(f"User {user_data['email']} created successfully.")
            else:
                print(f"Failed to create user {user_data['email']}: {response['error']}")
        except Exception as e:
            print(f"Error creating user {user_data['email']}: {e}")
```

---

## Example 4: Update an Existing User

Update details for an existing user.

```python
user_name = "mike.jones@example.com"
user_data = {
    "firstName": "Mikey",                                 # Optional: Update user's first name
    "lastName": "Jonesss",                                # Optional: Update user's last name
    "role": "designer",                                   # Optional: Role name to update; will be mapped to corresponding roleId
    "groups": ["mig_test"],                               # Optional: List of group names to update; will be mapped to corresponding group IDs
    "preferences": {                                      # Optional: Update user preferences, such as language settings
        "language": "fr-FR"
    }
}
response = access_mgmt.update_user(user_name, user_data)
print(json.dumps(response, indent=4))
```

---

## Example 5: Delete a User

Delete a user by their email.

```python
user_name = "john.doe@example.com"
response = access_mgmt.delete_user(user_name)
print(json.dumps(response, indent=4))
if "error" not in response:
    print(f"User {user_name} has been successfully deleted. Response: {response}")
else:
    print(f"Failed to delete user {user_name}. Error: {response['error']}")
```

---

## Example 6: Get All Users in a Specific Group

List all users in a given group.

```python
group_name = "Admins"
response = access_mgmt.users_per_group(group_name)

if isinstance(response, list):
    print(f"Found {len(response)} users in group '{group_name}':")
    for user in response:
        print(f"- {user.get('userName')}")
    df = api_client.to_dataframe(response)
    print(df)
elif isinstance(response, dict) and "error" in response:
    print(f"No users found in the group '{group_name}'. Error: {response['error']}")
else:
    print(f"Unexpected response structure: {response}")
```

---

## Example 7: Get All Groups with Associated Users

Get all groups and their associated users.

```python
response = access_mgmt.users_per_group_all()

if isinstance(response, list) and response:
    for group_info in response:
        group_name = group_info.get("group", "Unknown Group")
        usernames = group_info.get("username", [])
        print(f"Group: {group_name}")
        print("Usernames:", ", ".join(usernames) if usernames else "No users found")
        print("-" * 40)
else:
    error_msg = response.get("error") if isinstance(response, dict) else "Unexpected or empty response"
    print(f"No group-user associations found. Error: {error_msg}")

df = api_client.to_dataframe(response)
print(df)
api_client.export_to_csv(response, file_name="group_user_associations.csv")
```

---

## Example 8: Change Ownership of Folders and Dashboards

Transfer ownership of a folder and its dashboards.

```python
# Admin user executing the action
executing_user = "sisensepy@sisense.com"
# Name of the folder whose ownership will be changed
folder_to_transfer = "te1_sub"
# Email of the new owner
new_owner_email = "admin@sisense.com"
# Permission to assign back to the original owner ("edit" or "view")
original_owner_permission = "edit"
# Whether to transfer ownership of dashboards inside the folder to the new folder owner
include_dashboards = True

try:
    response = access_mgmt.change_folder_and_dashboard_ownership(
        executing_user=executing_user,
        folder_name=folder_to_transfer,
        new_owner_name=new_owner_email,
        original_owner_rule=original_owner_permission,
        change_dashboard_ownership=include_dashboards
    )
    if response and 'error' not in response:
        print("Folder ownership transferred successfully.")
        print(f"Total folders changed: {response.get('total_folders_changed', 0)}")
        print(f"Total dashboards changed: {response.get('total_dashboards_changed', 0)}")
    else:
        print(f"Failed to change ownership: {response.get('error', 'Unknown error')}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
```

---

## Example 9: Get Columns from a DataModel

Retrieve all columns from a specific DataModel.

```python
datamodel_name = "Sample ECommerce"
all_columns = access_mgmt.get_datamodel_columns(datamodel_name)
print(json.dumps(all_columns, indent=4))
df = api_client.to_dataframe(all_columns)
print(df)
```

---

## Example 10: Get Unused Columns in a DataModel

List unused columns in a DataModel.

```python
unused_columns = access_mgmt.get_unused_columns(datamodel_name='Sample ECommerce')
print(json.dumps(unused_columns, indent=4))
if unused_columns:
    df = api_client.to_dataframe(unused_columns)
    print(df)
api_client.export_to_csv(unused_columns, file_name="unused_columns.csv")
```

---

## Example 10B: Get Unused Columns in Multiple DataModels
```python
results = access_mgmt.get_unused_columns_bulk(
    datamodels=["60ca5fe3-dc7b-4db7-aaa4-7dff0ac30bcb", "MyDataModel_ec"]
)
print(json.dumps(results, indent=4))
if results:
    df = api_client.to_dataframe(results)
    print(df)
api_client.export_to_csv(results, file_name="unused_columns_bulk.csv")
```

---

## Example 11: Get Dashboard Shares Info

Get sharing information for all dashboards.

```python
dashboard_shares = access_mgmt.get_all_dashboard_shares()
df = api_client.to_dataframe(dashboard_shares)
print(df)
```

---

## Example 12: Create a Schedule Build for a DataModel in UTC

Create cron-based and interval-based schedules for DataModel builds.

**Cron-Based Schedule (specific days and time in UTC):**

```python
days = ["MON", "TUE", "FRI", "SAT", "SUN"]                      # Days of the week
hour = 21                                                       # 9 PM UTC
minute = 0                                                      # At the start of the hour
datamodel_name = 'pysense_databricks_ec'                        # Name of the DataModel

response = access_mgmt.create_schedule_build(
    days=days,
    hour=hour,
    minute=minute,
    datamodel_name=datamodel_name
)
if "error" not in response:
    print("Cron-based schedule created successfully:", response)
else:
    print("Failed to create schedule:", response["error"])
```

**Cron-Based Schedule for All Days:**

```python
days = ["*"]
hour = 2                                                        # 2 AM UTC
minute = 30
datamodel_name = 'pysense_databricks_ec'

response = access_mgmt.create_schedule_build(
    days=days,
    hour=hour,
    minute=minute,
    datamodel_name=datamodel_name,
    build_type="FULL"
)
if "error" not in response:
    print("Cron-based schedule created successfully:", response)
else:
    print("Failed to create schedule:", response["error"])
```

**Interval-Based Schedule (1 Hour):**

```python
datamodel_name = 'pysense_databricks_ec'

response = access_mgmt.create_schedule_build(
    datamodel_name=datamodel_name,
    interval_hours=1
)
if "error" not in response:
    print("Interval-based schedule created successfully:", response)
else:
    print("Failed to create schedule:", response["error"])
```

**Interval-Based Schedule (2 Days, 1 Hour, 4 Minutes):**

```python
datamodel_name = 'pysense_databricks_ec'

response = access_mgmt.create_schedule_build(
    datamodel_name=datamodel_name,
    interval_days=2,
    interval_hours=1,
    interval_minutes=4
)
if "error" not in response:
    print("Interval-based schedule created successfully:", response)
else:
    print("Failed to create schedule:", response["error"])
```

---

## Notes

- Adjust parameters as needed for your environment.
- For more details, refer to the documentation in the `docs/` folder.

---
