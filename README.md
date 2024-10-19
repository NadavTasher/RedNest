# RedNest
Redis dictionary & list nesting for Python objects using `redis-py` and Redis `Hash`es and `List`s

## Usage example
```python
import redis

from rednest import Dictionary, List

# Initialize redis connection
redis = redis.Redis(...)

# Create your dictionary
my_dict = Dictionary(redis, "test-dict")
my_dict.test_value = "Hello World"
my_dict.numbers = [10, 20, 30]
my_dict.ages = {
	"User 1": 10,
	"User 2": 20,
	"User 3": 30,
}

# Change a user age
my_dict.ages["User 3"] = 40

# Show the variable types
print(type(my_dict.ages), type(my_dict.numbers))

# Show the entire dictionary
print(my_dict)
```

## Starting a test server locally
To run a test server, you can use the following command to run a Redis server locally:
```bash
docker run --rm -it -p 6379:6379 redis/redis-stack-server
```