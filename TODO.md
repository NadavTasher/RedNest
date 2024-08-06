Pipeline transition:
## Generic
* Deletion list - instead of handling deletion at the time of update, do rpoplpush or something that moves the value (the nested key) to a global `delete` list. When all pipelines have been handled, process deletion (can also happen using an external deleter or something)
* Inserting new nested items does **NOT** require pipelines. It might be more efficient, but is not **REQUIRED**.

## Dictionary
* Set item as pipeline - not required

## List
* Set item as pipeline (+slice)
* Get item as pipeline (+slice as multi-get with / without pipeline)
* Delete item as pipeline (+slice)
* Insert item as pipeline (+insertion using rpush, lpush)
* Keys as multi-get?