#1 imports

from pywebhdfs.webhdfs import PyWebHdfsClient

#2 make connection with hadoop file system

hdfs = PyWebHdfsClient(user_name="hdfs",port=50070,host="sandbox-hdp.hortonworks.com")


hdfs.delete_file_dir('chapter5/LoanStats3d.csv',recursive=True)

#4 recreate the chapters directory

hdfs.make_dir('chapter5')

#5 upload the csv file

with open('./data/stored_csv.csv') as file_data:
	hdfs.create_file('chapter5/LoanStats3d.csv',file_data, overwrite=True)

#6 print the status to see if this succeeded.
print hdfs.get_file_dir_status('chapter5/LoanStats3d.csv')
