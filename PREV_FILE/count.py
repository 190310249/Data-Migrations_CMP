Table =  ['table_1', 'table_1', 'table_1', 'table_1', 'table_2', 'table_2', 'table_2', 'table_3', 'table_3', 'table_3', 'table_3', 'table_3']
#count_array = [0,1,1,1,0,2,2]  expect=[]
count_array = []
count=0
prev = ''
count_array.append('F')

print(len(Table))

for i in range(0,len(Table)-1):
    if Table[i] == Table[i+1]:
        count_array.append('T')
    else:
        count_array.append('F')

print(count_array)