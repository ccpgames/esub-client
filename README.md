# esub python client library

esub is a super lightweight micro message passing service,
this is a python client library to connect to an esub node

for more details about esub, see its repo: https://github.com/ccpgames/esub


## examples


# subscribing

```python
# this will block until fulfilled or canceled
print(esub.sub("some_key", token="secret"))
```

# replying

```python
# also blocking, but should be a fast response
esub.rep("some_key", "some data", token="secret")
```
