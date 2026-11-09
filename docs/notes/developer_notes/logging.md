# Logging Best Practices

## zrlog
zrlog is a tool I wrote that simplifies how logging is configured. It also adds two new logging levels
that are common in other applications: NOTICE and TRACE. You should use `zrlog.get_logger(NAME)` instead of
`logging.getLogger(NAME)` to have access to `.notice()` and `.trace()`.


## Logging Levels
Use the levels as follows:

### Error Conditions
- CRITICAL: The program is going to crash now.
- ERROR: The program has to abort what it is currently doing or received an exception.
- WARNING: The program is going to continue, but in a non-ideal manner that might need attention.

Note that ERROR and CRITICAL email the admins.

### Operational Output
- NOTICE: Mandatory logging for security and operational concerns (will be logged in prod)
- INFO: Non-mandatory logging of operational concerns (will not be logged in prod)

### Developer Output
- DEBUG: Information on general operations that can be reviewed to ensure proper operation and identify issues
- TRACE: An additional level of debugging that can be enabled for even more detail

## Formatting Arguments
Logging should not pre-format arguments - by delaying formatting arguments, you save on CPU time
if it isn't logged anyways. Especially important for debug() and trace().

### Good Practice

```python
import zrlog

def do_stuff():
    result = 'foobar'
    zrlog.get_logger('do_stuff').debug("Function result: %s", result)
```


### Bad Practice
```python
import zrlog

def do_stuff():
    result = 'foobar'
    zrlog.get_logger('do_stuff').debug(f"Function result: {result}")
```
