# Add the sqls here, if manually needs to be executed do add manual: True, else it will be generated based on the provided table_name.
dq_checks=[
    {
    "sql_check":"SELECT COUNT(*) FROM",
    "min_expect": 1,
    "manual": False
    },
    {
    "sql_check": "SELECT COUNT(*) as mac_users FROM songplays WHERE songplays.user_agent LIKE '%Mac OS%';",
    "min_expect": 1,
    "table_name": "songplays",
    "manual": True
    },
    {
    "sql_check": "SELECT COUNT(*) as windows_users FROM songplays WHERE songplays.user_agent LIKE '%Windows%';",
    "min_expect": 1,
    "table_name": "songplays",
    "manual": True
    }
]

def generate_dqs(table_list):
    """ Generates a data quality check query based on the table name provided. In future, complicated queries could also be attached to the above list which could to unique to a table """
    generated_dqs = []
    for dq in dq_checks:
        if not dq["manual"]:
            for table in table_list:
                temp_dict = {}
                temp_dict["sql_check"] = f"{dq['sql_check']} {table}"
                temp_dict["table_name"] = table
                temp_dict["min_expect"] = dq['min_expect']
                temp_dict["manual"] =  dq['manual']
                generated_dqs.append(temp_dict)
        else:
            generated_dqs.append(dq)
    return generated_dqs
