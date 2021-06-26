import os
import pandas as pd
import numpy as np

def SQL_Insert(SOURCE, TARGET):
    sql_texts = []
    for index, row in SOURCE.iterrows():
        sql_texts.append(
            'INSERT INTO ' + TARGET + ' (' + str(', '.join(SOURCE.columns)) + ')   VALUES ' + str(tuple(row.values))+";")

    return ('\n'.join(sql_texts))

def users_adapter(df):
        users_df = df.filter(['first_name', 'last_name', 'phone', 'email', 'start_date'])
        users_df['id'] = df.index+1
        users_df['club_id'] = club_id
        users_df.rename(columns={"start_date": "joined_at"}, inplace = True)
        users_df_reformatted = users_df.reindex(columns=['id', 'first_name', 'last_name', 'phone', 'email', 'joined_at', 'club_id'])
        return (users_df_reformatted)
    
def memberships_adapter(users_df_reformatted, df):
        memberships_df = df.filter(['user_id', 'start_date', 'end_date', 'membership_name'])
        memberships_df['id'] = df.index+1
        memberships_df['user_id'] = users_df_reformatted.id
        memberships_df_reformatted = memberships_df.reindex(columns=['id', 'user_id', 'start_date', 'end_date', 'membership_name'])
        return (memberships_df_reformatted)

# check if db file exists
if os.path.isfile("jimalaya.xlsx"):
    # read db file
    df = pd.read_excel('jimalaya.xlsx')
    # check for duplicated email in the file
    sr = df["email"]
    # make sure primary field is unique:
    if (sr.is_unique):
        club_id = '2400'

        # casting date 
        df["start_date"] = df.membershp_start_date.dt.strftime('%m/%d/%Y')
        df["end_date"] = df.membership_end_date.dt.strftime('%m/%d/%Y')

        # replacin nan values with None
        df = df.astype('object').where(pd.notnull(df),None)

        # manipulating data to match db dataframe
        users_df_reformatted = users_adapter(df)
        memberships_df_reformatted = memberships_adapter(users_df_reformatted, df)

        # generating create table statements
        print(pd.io.sql.get_schema(users_df_reformatted.reset_index(), 'users'))
        print(pd.io.sql.get_schema(memberships_df_reformatted.reset_index(), 'memberships'))

        # calling insert functions
        usersq = SQL_Insert(users_df_reformatted, 'users')
        membershipsq = SQL_Insert(memberships_df_reformatted, 'memberships')

        #print output
        print(usersq)
        print(membershipsq)
    else: 
        print("Email is duplicated")
else:
    print("File not found")
