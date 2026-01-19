from typing import List, Dict, Callable, TypeVar, Any
from statistics import mean, median
from collections import defaultdict

T = TypeVar('T')
AggregatorFunc = Callable[[List[T], Callable[[T], Any], Callable[[T], float]], List[T]]

def last_aggregator(items: List[T], key_func: Callable[[T], Any], value_func: Callable[[T], float]) -> List[T]:
    """Take the last item in each group - optimized for chronological data"""
    if not items:
        return []

    grouped = {}
    for item in items:
        group_key = key_func(item)
        grouped[group_key] = item

    result = []
    for group_key, item in grouped.items():
        new_item = type(item)(x=group_key, y=value_func(item))
        result.append(new_item)

    return result

def first_aggregator(items: List[T], key_func: Callable[[T], Any], value_func: Callable[[T], float]) -> List[T]:
    """Take the first item in each group"""
    if not items:
        return []

    grouped = {}
    for item in items:
        group_key = key_func(item)
        if group_key not in grouped:
            grouped[group_key] = item

    result = []
    for group_key, item in grouped.items():
        new_item = type(item)(x=group_key, y=value_func(item))
        result.append(new_item)

    return result

def sum_aggregator(items: List[T], key_func: Callable[[T], Any], value_func: Callable[[T], float]) -> List[T]:
    """Sum values in each group using native sum()"""
    if not items:
        return []

    grouped = defaultdict(list)
    templates = {}

    for item in items:
        group_key = key_func(item)
        grouped[group_key].append(value_func(item))
        if group_key not in templates:
            templates[group_key] = item

    result = []
    for group_key, values in grouped.items():
        template = templates[group_key]
        new_item = type(template)(x=group_key, y=sum(values))
        result.append(new_item)

    return result

def avg_aggregator(items: List[T], key_func: Callable[[T], Any], value_func: Callable[[T], float]) -> List[T]:
    """Average values in each group using native mean()"""
    if not items:
        return []
    
    grouped = defaultdict(list)
    templates = {}
    
    for item in items:
        group_key = key_func(item)
        grouped[group_key].append(value_func(item))
        if group_key not in templates:
            templates[group_key] = item
    
    result = []
    for group_key, values in grouped.items():
        template = templates[group_key]
        new_item = type(template)(x=group_key, y=mean(values))
        result.append(new_item)
    
    return result

def median_aggregator(items: List[T], key_func: Callable[[T], Any], value_func: Callable[[T], float]) -> List[T]:
    """Median values in each group using native median()"""
    if not items:
        return []
    
    grouped = defaultdict(list)
    templates = {}
    
    for item in items:
        group_key = key_func(item)
        grouped[group_key].append(value_func(item))
        if group_key not in templates:
            templates[group_key] = item
    
    result = []
    for group_key, values in grouped.items():
        template = templates[group_key]
        new_item = type(template)(x=group_key, y=median(values))
        result.append(new_item)
    
    return result

def max_aggregator(items: List[T], key_func: Callable[[T], Any], value_func: Callable[[T], float]) -> List[T]:
    """Max values in each group using native max()"""
    if not items:
        return []
    
    grouped = defaultdict(list)
    templates = {}
    
    for item in items:
        group_key = key_func(item)
        grouped[group_key].append(value_func(item))
        if group_key not in templates:
            templates[group_key] = item
    
    result = []
    for group_key, values in grouped.items():
        template = templates[group_key]
        new_item = type(template)(x=group_key, y=max(values))
        result.append(new_item)
    
    return result

def min_aggregator(items: List[T], key_func: Callable[[T], Any], value_func: Callable[[T], float]) -> List[T]:
    """Min values in each group using native min()"""
    if not items:
        return []
    
    grouped = defaultdict(list)
    templates = {}
    
    for item in items:
        group_key = key_func(item)
        grouped[group_key].append(value_func(item))
        if group_key not in templates:
            templates[group_key] = item
    
    result = []
    for group_key, values in grouped.items():
        template = templates[group_key]
        new_item = type(template)(x=group_key, y=min(values))
        result.append(new_item)
    
    return result

# Map of aggregator names to functions
AGGREGATORS: Dict[str, AggregatorFunc] = {
    'last': last_aggregator,
    'first': first_aggregator,
    'sum': sum_aggregator,
    'avg': avg_aggregator,
    'median': median_aggregator,
    'max': max_aggregator,
    'min': min_aggregator,
}
