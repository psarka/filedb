import abc
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Union

from filedb.key import Value


class MongoQueryError(Exception):
    pass


class BSONType(Enum):
    Double = 'double'
    String = 'string'
    Object = 'object'
    Array = 'array'
    BinaryData = 'binData'
    ObjectId = 'objectId'
    Boolean = 'bool'
    Date = 'date'
    Null = 'null'
    RegularExpression = 'regex'
    Int32 = 'int'
    Int64 = 'long'
    Decimal128 = 'decimal'
    Number = 'number'


class _Constraint(abc.ABC):

    def __and__(self, other: '_Constraint'):
        return _AllConstraint([self, other])

    def __or__(self, other: '_Constraint'):
        return _AnyConstraint([self, other])

    def __invert__(self):
        return _NotConstraint(self)

    @classmethod
    def parse(cls, constraint: 'Constraint') -> '_Constraint':
        if isinstance(constraint, _Constraint):
            return constraint
        elif isinstance(constraint, dict):
            return _DictConstraint({k: _Operator.parse(v) for k, v in constraint.items()})

    @abc.abstractmethod
    def value(self):
        ...


class _DictConstraint(_Constraint):
    def __init__(self, operator_dict: Dict[str, '_Operator']):
        self.operator_dict = operator_dict

    def value(self):
        return {k: v.value() for k, v in self.operator_dict.items()}


class _NotConstraint(_Constraint):
    def __init__(self, const: _Constraint):
        self.const = const

    def value(self):
        return {'$not': self.const.value()}


class _AnyConstraint(_Constraint):
    def __init__(self, consts: List[_Constraint]):
        self.consts = consts

    def value(self):
        return {'$or': [c.value() for c in self.consts]}


class _AllConstraint(_Constraint):
    def __init__(self, consts: List[_Constraint]):
        self.consts = consts

    def value(self):
        return {'$and': [v.value() for v in self.consts]}


class _Operator(abc.ABC):

    def __and__(self, other):

        def join_ops(ops):

            op_kws = sorted([op.keyword for op in ops])

            duplicated = set()
            for prev, succ in zip(op_kws[1:], op_kws[:-1]):
                if prev == succ:
                    duplicated.add(prev)

            if duplicated:
                raise MongoQueryError(f'Cannot "&" join the same operator(s): {duplicated}')
            else:
                return _AllOperator(ops)

        if isinstance(self, _AllOperator) and isinstance(other, _AllOperator):
            return join_ops(self.ops + other.ops)
        elif isinstance(self, _AllOperator):
            return join_ops(self.ops + [other])
        elif isinstance(other, _AllOperator):
            return join_ops([self] + other.ops)
        else:
            return join_ops([self, other])

    def __invert__(self):
        return _NotOperator(self)

    @classmethod
    def parse(cls, operator: 'Operator'):
        if isinstance(operator, _Operator):
            return operator
        else:
            return _Value(operator)

    @abc.abstractmethod
    def value(self):
        ...

    @property
    @abc.abstractmethod
    def keyword(self):
        ...


class _Value(_Operator):
    keyword = ''

    def __init__(self, value_: Value):
        self.value_ = value_

    def value(self):
        return self.value_


class _NotOperator(_Operator):
    keyword = '~'

    def __init__(self, op: _Operator):
        self.op = op

    def value(self):
        return {'$not': self.op.value()}


class _AllOperator(_Operator):
    keyword = ''

    def __init__(self, ops: List[_Operator]):
        self.ops = ops

    def value(self):
        v = {}
        for op in self.ops:
            v.update(op.value())
        return v


class _TypeComparisonOperator(_Operator):
    keyword = 'type'

    def __init__(self, type_: BSONType):
        self.type_ = type_

    def value(self):
        return {'$type': self.type_.value}


class _SingleComparisonOperator(_Operator):
    def __init__(self,
                 op_string: str,
                 keyword: str,
                 value_: Value):
        self.op_string = op_string
        self.value_ = value_
        self.keyword_ = keyword

    def value(self):
        return {self.op_string: self.value_}

    @property
    def keyword(self):
        return self.keyword_


class _MultipleComparisonOperator(_Operator):
    def __init__(self,
                 op_string: str,
                 keyword: str,
                 values: List[Value]):
        self.op_string = op_string
        self.values = values
        self.keyword_ = keyword

    def value(self):
        return {self.op_string: self.values}

    @property
    def keyword(self):
        return self.keyword_


class _Exists(_Operator):
    keyword = 'exists/not_exists'

    def __init__(self, yes_or_no: bool):
        self.yes_or_no = yes_or_no

    def value(self):
        return {'$exists': self.yes_or_no}


class _Query:

    @staticmethod
    def any(*args: 'Constraint'):
        args = [a if isinstance(a, _Constraint) else _DictConstraint(a) for a in args]
        return _AnyConstraint(args)

    @staticmethod
    def all(*args: 'Constraint'):
        args = [a if isinstance(a, _Constraint) else _DictConstraint(a) for a in args]
        return _AllConstraint(args)

    @staticmethod
    def equal(value):
        return _SingleComparisonOperator('$eq', 'equal', value)

    @staticmethod
    def not_equal(value):
        return _SingleComparisonOperator('$ne', 'not_equal', value)

    @staticmethod
    def greater_than(value):
        return _SingleComparisonOperator('$gt', 'greater_than', value)

    @staticmethod
    def greater_or_equal(value):
        return _SingleComparisonOperator('$gte', 'greater_or_equal', value)

    @staticmethod
    def less_than(value):
        return _SingleComparisonOperator('$lt', 'less_than', value)

    @staticmethod
    def less_or_equal(value):
        return _SingleComparisonOperator('$lte', 'less_or_equal', value)

    @staticmethod
    def is_in(values):
        return _MultipleComparisonOperator('$in', 'is_in', values)

    @staticmethod
    def not_in(values):
        return _MultipleComparisonOperator('$nin', 'not_in', values)

    @property
    def exists(self):
        return _Exists(True)

    @property
    def not_exists(self):
        return _Exists(False)

    @staticmethod
    def has_type(type_: BSONType):
        return _TypeComparisonOperator(type_)


def expand(x: 'Query'):
    return _Constraint.parse(x).value()


q = _Query()

Operator = Union[_Operator, Value]
Constraint = Union[_Constraint, Dict[str, Operator]]
DSLMongoQuery = Constraint
RawMongoQuery = Dict[str, Union[Value,
                                List[Value],
                                List['RawMongoQuery'],
                                'RawMongoQuery']]  # not sure
Query = Union[RawMongoQuery, Constraint]
