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
        usersdf = df.filter(['first_name', 'last_name', 'phone', 'email', 'start_date'])
        usersdf['id'] = df.index+1
        usersdf['club_id'] = club_id
        usersdf.rename(columns={"start_date": "joined_at"}, inplace = True)
        usersdf_reformatted = usersdf.reindex(columns=['id', 'first_name', 'last_name', 'phone', 'email', 'joined_at', 'club_id'])
        return (usersdf_reformatted)
    
def memberships_adapter(usersdf_reformatted, df):
        membershipsdf = df.filter(['user_id', 'start_date', 'end_date', 'membership_name'])
        membershipsdf['id'] = df.index+1
        membershipsdf['user_id'] = usersdf_reformatted.id
        membershipsdf_reformatted = membershipsdf.reindex(columns=['id', 'user_id', 'start_date', 'end_date', 'membership_name'])
        return (membershipsdf_reformatted)

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
        usersdf_reformatted = users_adapter(df)
        membershipsdf_reformatted = memberships_adapter(usersdf_reformatted, df)

        # generating create table statements
        print(pd.io.sql.get_schema(usersdf_reformatted.reset_index(), 'users'))
        print(pd.io.sql.get_schema(membershipsdf_reformatted.reset_index(), 'memberships'))

        # calling insert functions
        usersq = SQL_Insert(usersdf_reformatted, 'users')
        membershipsq = SQL_Insert(membershipsdf_reformatted, 'memberships')

        #print output
        print(usersq)
        print(membershipsq)
    else: 
        print("Email is duplicated")
else:
    print("File not found")
