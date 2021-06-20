import os
import pandas as pd
import numpy as np

def SQL_Insert(SOURCE, TARGET):
    sql_texts = []
    for index, row in SOURCE.iterrows():
        sql_texts.append(
            'INSERT INTO ' + TARGET + ' (' + str(', '.join(SOURCE.columns)) + ')   VALUES ' + str(tuple(row.values))+";")

    return ('\n'.join(sql_texts))

# check if db file exists
if os.path.isfile("jimalaya.xlsx"):
    # read db file
    df = pd.read_excel('jimalaya.xlsx')
    # check for duplicated email in the file
    sr = df["email"]
    # if primary field is unique:   
    if (sr.is_unique):
        club_id = input("Enter club's ID:") or '2400'
        # casting date 
        df["start_date"] = df.membershp_start_date.dt.strftime('%m/%d/%Y')
        df["end_date"] = df.membership_end_date.dt.strftime('%m/%d/%Y')
        # replacin nan with None
        df = df.astype('object').where(pd.notnull(df),None)
        # manipulating users data to match db dataframe
        usersdf = df.filter(['first_name', 'last_name', 'phone', 'email', 'start_date'])
        usersdf['id'] = df.index+1
        usersdf['club_id'] = club_id
        usersdf.rename(columns={"start_date": "joined_at"}, inplace = True)
        # reindex users columns
        usersdf_reindexed = usersdf.reindex(columns=['id', 'first_name', 'last_name', 'phone', 'email', 'joined_at', 'club_id'])

        # manipulating membnerships data to match db dataframe
        membershipsdf = df.filter(['user_id', 'start_date', 'end_date', 'membership_name'])
        membershipsdf['id'] = df.index+1
        membershipsdf['user_id'] = usersdf.id
        # reindex memberships columns
        membershipsdf_reindexed = membershipsdf.reindex(columns=['id', 'user_id', 'start_date', 'end_date', 'membership_name'])

        # generating create table statements
        print(pd.io.sql.get_schema(usersdf_reindexed.reset_index(), 'users'))
        print(pd.io.sql.get_schema(membershipsdf_reindexed.reset_index(), 'memberships'))

        # calling insert functions
        usersq = SQL_Insert(usersdf_reindexed, 'users')
        membershipsq = SQL_Insert(membershipsdf_reindexed, 'memberships')

        #print output
        print(usersq)
        print(membershipsq)
    else: 
        print("Email is duplicated")
else:
    print("File not found")