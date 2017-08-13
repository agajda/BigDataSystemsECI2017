The querys are in the "PigQuery.pig" file.

To run the example type:
	pig -x local PigQuery.pig 2> /dev/null

There are two files with name "first.txt" and "second.txt".
The first file contain three fields: user, url & id.
The second file contain two fields: url & rating. 
These two files are CSV files.

The script performs the following simple queries:

1) Load the two inputs files.
2) Remove a field from 'loading1'.
3) Filter the record from 'loading1' when the condition 'id' is greater than 8. 
4) Join the two relations based on the column 'url' from 'loading1' and 'loading2'.
5) Sort data in 'loading2' in ascending order on ratings field.
6) Save the ordered data into de 'output' directory.
