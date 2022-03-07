# Table =  ['table_1', 'table_1', 'table_1', 'table_1', 'table_2', 'table_2', 'table_2', 'table_3', 'table_3', 'table_3', 'table_3', 'table_3']
# #count_array = [0,1,1,1,0,2,2]  expect=[]
# count_array = []
# count=0
# prev = ''
# count_array.append('F')

# print(len(Table))

# for i in range(0,len(Table)-1):
#     if Table[i] == Table[i+1]:
#         count_array.append('T')
#     else:
#         count_array.append('F')

# print(count_array)

#"COPY table_1 FROM 's3://parquet-bucket-sfs/table_1/userdata8.parquet' IAM_ROLE 'arn:aws:iam::143580737085:role/migrationrole' FORMAT AS PARQUET;"
#"COPY table FROM s3://parquet-bucket-sfs/parquet_buck IAM_ROLE arn:aws:iam::143580737085:role/migrationrole FORMAT AS PARQUET;"
copy_command = ('"'+'COPY '+'table'+ ' FROM '+"'"+'s3://parquet-bucket-sfs/'+'parquet_buck'+' IAM_ROLE arn:aws:iam::143580737085:role/migrationrole'+"'"+' FORMAT AS PARQUET;'+'"')
print(str(copy_command))