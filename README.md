# func-dataframe
An extension of the classic Pandas DataFrame with function-enabled columns.

# Documentation
## FuncDataFrame
The package provides a DataFrame accessor for function-enabled columns, with (optional) parallel computation provided by Dask.

Usage
```python
def plus(a):
    return a+1

df = pd.DataFrame({'a':list(range(5)),'b':list(range(5))})
df = df.fdf.compute({'c':plus})
print(df)
```
```
Output:
Applying plus...
100%|████████████████████████████████████████████████████████████████████████████████████| 5/5 [00:00<00:00, 591.36it/s]
   a  b  c
0  0  0  1
1  1  1  2
2  2  2  3
3  3  3  4
4  4  4  5
```
Value-dependencies between functions are automatically resolved if needed:
```python
def plus(a):
    return a+1
def d_needed(d):
    return d+1

df = pd.DataFrame({'a':list(range(5)),'b':list(range(5))})
df = df.fdf.compute({'e':d_needed, 'd':plus}, parallel=True, n_dask_partitions=10)
print(df)
```
```
Output:
Applying d_needed...
[########################################] | 100% Completed | 101.81 ms
Applying plus...
[########################################] | 100% Completed | 101.83 ms
   a  b  e  d
0  0  0  2  1
1  1  1  3  2
2  2  2  4  3
3  3  3  5  4
4  4  4  6  5
```
## fdf_func
fdf_func is a decorator for the functions you want to add in the dataframe.
You can decorate any function, and specify two behaviours:
- a mapping between function args and dataframe columns,
- functions args to ignore (you need to have a default value for them).
```python
def plus(a):
    return a+1
@fdf_func(c='d')
def d_needed(c):
    return c+1

df = pd.DataFrame({'a':list(range(5)),'b':list(range(5))})
df = df.fdf.compute({'e':d_needed, 'd':plus}, parallel=True, n_dask_partitions=5, shuffle=True)
print(df)
```
```
Output:
Applying d_needed...
[########################################] | 100% Completed | 101.79 ms
Applying plus...
[########################################] | 100% Completed | 101.66 ms
   a  b  e  d
0  1  1  3  2
1  3  3  5  4
2  4  4  6  5
3  2  2  4  3
4  0  0  2  1
```
