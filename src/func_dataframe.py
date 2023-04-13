from pandas import DataFrame
from inspect import signature

def func_df(ignore_args = {}, **kwargs):
  def decorator(func):
    func_kwargs = {}
    for p in signature(func).parameters.values():
      #if p.kind == inspect.Parameter.POSITIONAL_ONLY or p.kind == inspect.Parameter.VAR_POSITIONAL:
        #raise ValueError(f"[{func.__name__}] Only keyword arguments are allowed")
      if p.name in ignore_args:
        continue
      func_kwargs[p.name] = p.name
      
    if not kwargs.keys() <= func_kwargs.keys():
      raise ValueError(f"[{func.__name__}] Invalid keyword arguments in func_df decorator")
    
    func_kwargs.update(kwargs)
    func.func_df_dict = func_kwargs
    return func
  return decorator


class FuncDataFrame(DataFrame):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.funcs = {}
    
  def decorate_func(self, func, column):
    def wrapper(row):
      #print(f"Row: {row}")
      if row[column] is not None:
        return row
      for c in func.func_df_dict:
        if c in self.funcs:
          row = self.funcs[c](row)
      
      func_kwargs = {}
      #print(f"func.func_df_dict: {func.func_df_dict}")
      for c in func.func_df_dict:
        if func.func_df_dict[c] not in row:
          raise ValueError(f"[{func.__name__}] Column '{func.func_df_dict[c]}' not found in the dataframe")
        func_kwargs[c] = row[func.func_df_dict[c]]
      #print(f"row[column]: {row[column]}")
      #print(f"func_kwargs: {func_kwargs}")
      #print(f"func(func_kwargs): {func(**func_kwargs)}")
      row[column] = func(**func_kwargs)
      #print(f"row: {row}")
      return row
    return wrapper
      
  def add_cf(self, cf_dict = None, column = None, func = None):
    if (not column or not func) and not cf_dict:
      #print(not column)
      #print(not func)
      raise ValueError("Either column and func or cf_dict must be provided")
    if (column or func) and cf_dict:
      raise ValueError("Either column and func or cf_dict must be provided, not both")
    
    if column and func:
      cf_dict = {column: func}
    for column in cf_dict:
      if not hasattr(cf_dict[column], 'func_df_dict'):
        cf_dict[column] = func_df()(cf_dict[column])
        #raise ValueError(f"Function {cf_dict[column].__name__} is not decorated")
    for column in cf_dict:
      cf_dict[column] = self.decorate_func(cf_dict[column], column)
    self.funcs.update(cf_dict)

    for column in cf_dict:
      self[column] = None
    
  def del_cf(self, column):
    if column in self.funcs:
      del self.funcs[column]
      self.drop(column, axis=1, inplace=True)
      
  def rename_cf(self, old_column, new_column):
    if old_column in self.funcs:
      self.funcs[new_column] = self.funcs[old_column]
      del self.funcs[old_column]
      self.rename(columns={old_column: new_column}, inplace=True)
      
  def update_cf_df(self, column = None, parallel = False):
    if column:
      if column not in self.funcs:
        raise ValueError(f"Column {column} not found in funcs")
      if parallel:
        import swifter
        self[self.columns] = self.swifter.apply(self.funcs[column], axis = 1)
      else:
        self[self.columns] = self.apply(self.funcs[column], axis = 1)	
    else:
      for column, func in self.funcs.items():
        if parallel:
          import swifter
          self[self.columns] = self.swifter.apply(func, axis = 1)
        else:
          self[self.columns] = self.apply(func, axis = 1)