

submit = DataFrame(o.get_table('meinian_submit') ).to_pandas()
print(submit.head())
submit.set_index('vid',inplace=True)
from odps.models import Schema, Column, Partition

Y_ans0 = o.get_table('odps_tc_257100_f673506e024.meinian_round2_submit_b') 
records = []

with Y_ans0.open_reader() as reader:
    count1 = reader.count
    print 'For original table'
    print count1
    for record in reader[0:2]:
    	print record
    for record in reader[0:count1]:
        vid = record[0]
    	records.append([vid, submit.loc[vid,'sys'], submit.loc[vid,'dia'], submit.loc[vid,'tl'], submit.loc[vid,'hdl'], submit.loc[vid,'ldl']])

columns = [Column(name='vid', type='String', comment='体检人id'),
	Column(name='sys', type='bigint', comment='收缩压'),
    Column(name='dia', type='bigint', comment='舒张压'),
    Column(name='tl', type='double', comment='甘油三酯'),
    Column(name='hdl', type='double', comment='高密度脂蛋白胆固醇'),
    Column(name='ldl', type='double', comment='低密度脂蛋白胆固醇')]
schema = Schema(columns=columns)

o.delete_table('meinian_round2_submit_b', if_exists=True)
tableS = o.create_table('meinian_round2_submit_b', schema, if_not_exists=True)
writer = tableS.open_writer()
writer.write(records)
writer.close()


