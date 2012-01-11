import redis
import simplejson
import uuid, time
import logging

TASK_NEW = 'NEW'
TASK_RUNNING = 'RUNNING'
TASK_DONE = 'DONE'
TASK_RETRY = 'RETRY'

logger = logging.getLogger("RediQue")

class JSONSerializer(simplejson.JSONEncoder):
    '''
    A JSON encoder that is capable of encoding Exceptions
    '''
    def default(self, obj):
        if isinstance(obj, Exception):
            return str(obj)
        return simplejson.JSONEncoder.default(self, obj)
    
class RediQue(object):
    def __init__(self, queue, prefix="redique", serializer=simplejson, timeout=0, **kwargs):
        '''
        Initialize new RediQue instance, 
        you can specify the host, port, db as a normal redis.Redis() instance
        '''
        self._redis = redis.Redis(**kwargs)
        self._queue = "%s:%s" % (prefix, queue)
        self._serializer = serializer
        self._timeout = timeout
    
    def _encode_task(self, func_obj, args, kwargs):
        if isinstance(func_obj, basestring):
            task_name = func_obj
        else:
            task_name = func_obj.__name__
        task = {'id': uuid.uuid4().hex,
                'task_func': task_name,
                'args' : args,
                'kwargs' : kwargs
                }
        
        return (task['id'], self._serializer.dumps(task, cls=JSONSerializer))
    
    def _get_task_result(self, task_id, isblocking):
        task_key = "%s:%s" % (self._queue, task_id)
        result_key = "%s:%s:result" % (self._queue, task_id)
        if isblocking:
            result = self._redis.blpop(result_key, timeout=self._timeout)
            if result:
                #interesting that in this case a tuple is returned!
                result = result[1]
        else:
            result = self._redis.lpop(result_key)
        if result:
            self._redis.delete(result_key)
            self._redis.delete(task_key)
        else:
            return None
        res = self._serializer.loads(result)
        if 'err' in res:
            raise Exception(res['err'])
        return res['result']
    
    def _render_result(self, result):
        return {'result': result}
    
    def _render_error(self, exception):
        return {'err': exception}
    
    def flush(self):
        '''
        removes all keys related to this particular queue
        '''
        with self._redis.pipeline(transaction=True, shard_hint=None) as pipe:
            for key in self._redis.keys("%s*" % self._queue):
                pipe.delete(key)
            pipe.execute()
            
    def push_task(self, func_obj, *args, **kwargs):
        '''
        pushes the task asynchronously to the queue and returns a task_id which you can use to track the progress of the task using get_task_state()
        '''
        task_id, task = self._encode_task(func_obj, args, kwargs)
        key = "%s:%s" % (self._queue, task_id)
        logger.debug("Pushing task '%s' to the queue '%s'", task_id, self._queue)
        with self._redis.pipeline(transaction=True, shard_hint=None) as pipe:
            pipe.rpush(self._queue, task).hset(key, "state", TASK_NEW).hset(key, "timestamp", time.time())
            pipe.execute()
        return task_id
    
    
    def read_task_result(self, task_id):
        '''
        returns the task result if it was ready, please note that this removes the result from the database, so you can read the result only once per task.
        returns None if task was not found or result is not ready
        '''
        return self._get_task_result(task_id, isblocking=False)
    
    def wait_task_result(self, task_id, remove=True):
        '''
        blocks till the result is ready then returns that result, also notice that this removes the result from the database
        '''
        return self._get_task_result(task_id, isblocking=True)
        
    def execute_task(self, func_obj, *args, **kwargs):
        '''
        blocks will the task is executed by one of the workers, returns the result of the task
        '''
        task_id = self.push_task(func_obj, *args, **kwargs)
        return self.wait_task_result(task_id, remove=True)
    
    def publish_message(self, channel, message):
        #TODO implement pubsub
        pass
    
    def get_task_state(self, task_id):
        '''
        returns task state as a dict {'state': 'NEW', 'timestamp': 1317199118.683899}
        The state key indicates the current state of the task
        The timestamp indicates when this state was changed
        '''
        result = self._redis.hgetall("%s:%s" % (self._queue, task_id))
        if result:
            logging.debug("Task '%s' was set to status %s on %s", task_id, result['state'], result['timestamp'])
            return result
        else:
            return None
    
    def get_queue_length(self):
        '''
        returns the total number of tasks in this queue (pending tasks), a better way to do this is to use len(redique_instance)
        '''
        return self._redis.llen(self._queue)
    
    def consume_one(self, backend):
        '''
        consume only one task from the queue and execute tasks on the given backend instance, this blocks till at least one task is available
        this returns the result of the task on the queue for the client to pick up.
        '''
        data = self._redis.blpop(self._queue, self._timeout)
        if not data:
            return None
        task = self._serializer.loads(data[1])
        task_id = task['id']
        result_key = "%s:%s:result" % (self._queue, task_id)
        task_key = "%s:%s" % (self._queue, task_id)
        logger.debug("Consuming task '%s', operation %s", task_id, task['task_func'])
        try:
            method = getattr(backend, task['task_func'])
            self._redis.hset(task_key, 'state', TASK_RUNNING)
            self._redis.hset(task_key, 'timestamp', time.time())
            t1 = time.time()
            result = self._render_result(method(*task['args'], **task['kwargs']))
            t2 = time.time()
            logger.debug("Task '%s' was executed in %.4f seconds", task_id, t2-t1)
        except AttributeError, e:
            result = self._render_error(e)
        except Exception, e:
            result = self._render_error(e)
        self._redis.lpush(result_key, self._serializer.dumps(result, cls=JSONSerializer))
        task_key = "%s:%s" % (self._queue, task_id)
        self._redis.hset(task_key, "state", TASK_DONE)
        self._redis.hset(task_key, "timestamp", time.time())
        return task_id
    
    def consume_loop(self, backend):
        '''
        basically, calls consume_one() in an infinite loop.
        '''
        logger.info("Consumer started, waiting for tasks on queue '%s'", self._queue)
        while True:
            self.consume_one(backend)
    
    def __len__(self):
        return self.get_queue_length()
