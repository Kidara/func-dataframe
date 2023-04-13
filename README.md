# func-dataframe
An extension of the classic Pandas DataFrame with function-enabled columns.

# Documentation
## FuncDataFrame
The package simply extend the standard Pandas DataFrame, hence all operations on DataFrames are supported.
It it straighforward to substitute Pandas DataFrames with FuncDataFrames:
```python
fdf = FuncDataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
print(fdf)
```
```
Output:
   a  b
0  1  4
1  2  5
2  3  6
```
You can add, delete and rename function-enabled columns:
```python
fdf.add_cf(column='c', func=lambda a,b: a+b)
print(fdf, end='\n\n')

fdf.rename_cf(old_column='c', new_column='cc')
print(fdf, end='\n\n')

fdf.del_cf(column='cc')
print(fdf)
```
```
Output:
   a  b     c
0  1  4  None
1  2  5  None
2  3  6  None

   a  b    cc
0  1  4  None
1  2  5  None
2  3  6  None

   a  b
0  1  4
1  2  5
2  3  6
```
You can update the values of function-enabled columns using non-parallel or parallel (thanks to [swifter](https://github.com/jmcarpenter2/swifter)) computation:
```python
fdf.add_cf(column='c', func=lambda a,b: a+b)
fdf.update_cf_df(parallel=True)
print(fdf)
```
```
Output:
   a  b  c
0  1  4  5
1  2  5  7
2  3  6  9
```
Value-dependencies between functions are automatically resolved if needed:
```python
fdf.add_cf(column='c', func=lambda a,b: a+b)
fdf.add_cf(column='d', func=lambda a,b: a*b)
fdf.update_cf_df(column='d')
print(fdf, end='\n\n')

fdf.add_cf(column='d', func=lambda a,c: a*c)
fdf.update_cf_df(column='d')
print(fdf)
```
```
Output:
   a  b     c   d
0  1  4  None   4
1  2  5  None  10
2  3  6  None  18

   a  b  c   d
0  1  4  5   5
1  2  5  7  14
2  3  6  9  27
```
Results coherence has not yet been implemented. Therefore, inconsistencies can occur if you change any column after updating the dataframe.
## func_df
func_df is a decorator for the functions you want to add in the dataframe.
You can decorate any function, and specify two behaviours:
- a mapping between function args and dataframe columns,
- functions args to ignore (you need to have a default value for them).
```python
@func_df(bb='b', ignore_args={'x'})
def f1(a, x=10, bb=None):
  return a * x * bb

fdf.add_cf(column='c', func=f1)
fdf.update_cf_df(column='c')
print(fdf)
```
```
Output:
   a  b    c
0  1  4   40
1  2  5  100
2  3  6  180
```
