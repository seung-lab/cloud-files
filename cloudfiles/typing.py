from typing import (
  Any, Dict, Optional, 
  Union, Tuple,  
  Iterable, TypeVar
)

T = TypeVar('T')
ScalarOrIterable = Union[T, Iterable[T]]
CompressType = Optional[Union[str,bool]]
GetPathType = ScalarOrIterable[str]
PutScalarType = Union[Tuple[str,bytes], Dict[str,Any]]
PutType = ScalarOrIterable[PutScalarType]
ParallelType = Union[int,bool]
SecretsType = Optional[Union[str,dict]]