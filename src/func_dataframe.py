from pandas import DataFrame
from pandas.api.extensions import register_dataframe_accessor
from inspect import signature
from functools import reduce

def fdf_func(ignore_args = {}, **kwargs):
  def decorator(func):
    func_kwargs = {}
    for p in signature(func).parameters.values():
      if p.name in ignore_args:
        continue
      func_kwargs[p.name] = p.name
      
    if not kwargs.keys() <= func_kwargs.keys():
      raise ValueError(f"[{func.__name__}] Invalid keyword arguments in fdf_func decorator")
    
    func_kwargs.update(kwargs)
    func.func_df_dict = func_kwargs
    return func
  return decorator

  
def _dask_map(func):
  import func_dataframe
  def _inner(partition, func=func):
    return partition.apply(func, axis=1)
  return _inner

@register_dataframe_accessor("fdf")
class FuncDataFrame:
  def __init__(self, pandas_obj):
    self.obj = pandas_obj
  
  def decorate_func(self, func, columns):
    for c in func.func_df_dict:
      if func.func_df_dict[c] not in self.obj:
        raise ValueError(f"[{func.__name__}] Column '{func.func_df_dict[c]}' not found in the dataframe")
    def wrapper(row, columns=columns):
      if isinstance(columns, tuple):
        columns = list(columns)
        if row[columns].apply(lambda x: x != None).all():
          return row
      else:
        if row[[columns]].apply(lambda x: x != None).all():
          return row
      
      for c in func.func_df_dict.values():
        if c in self.cc and row[c] is None:
          for cs in self.cf_dict:
            if c in cs:
              row = self.cf_dict[cs](row)
      
      func_kwargs = {}
      for c in func.func_df_dict:
        func_kwargs[c] = row[func.func_df_dict[c]]
      if len(columns) == 1:
        row[columns] = func(**func_kwargs)
      else:
        row[columns] = func(**func_kwargs)
      return row
    wrapper.__name__ = func.__name__
    return wrapper
    
  def compute(self, cf_dict, parallel=False, n_dask_partitions=None, shuffle=False, scheduler='threads'):
    cf_dict = cf_dict.copy()
    
    cc = list(cf_dict.keys())
    
    self.cc = reduce(lambda c1, c2: c1 + c2, map(lambda c: (c,) if not isinstance(c, tuple) else c, cf_dict.keys()))
    for c in self.cc:
      self.obj[c] = None
    
    for c in cf_dict:
      if not hasattr(cf_dict[c], 'func_df_dict'):
        cf_dict[c] = fdf_func()(cf_dict[c])
      cf_dict[c] = self.decorate_func(cf_dict[c], c)
    
    self.cf_dict = cf_dict

    if shuffle:
      self.obj = self.obj.sample(frac=1).reset_index(drop=True)
    for func in cf_dict.values():
      print(f"Applying {func.__name__}...")
      if parallel:
        from dask import dataframe as dd
        from dask.diagnostics import ProgressBar
        with ProgressBar():
          self.obj[:] = dd.from_pandas(self.obj, npartitions=n_dask_partitions).map_partitions(lambda p, f=func: p.apply(f, axis=1)).compute(scheduler=scheduler)
      else:
        from tqdm import tqdm
        tqdm.pandas()
        self.obj[:] = self.obj.progress_apply(func, axis = 1)
        
    return self.obj