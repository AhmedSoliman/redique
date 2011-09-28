# Introduction

*redique* is an implementation of a high-performance Async RPC/Task Queue system built on top of Redis datastructure store 
and JSON marshalling protocol.

You normally want to use *redique* when you need to publish tasks to a set of workers to process asynchronously and retrieve the result using a task_id, or 
when you want to distribute workload over multiple workers easily without going through the hassle of understanding how message buses work.

# Getting Started

You need to install the package first using pip
> pip install redique

Then you need to create a backend class that contains the actual logic you want implement over the transport

	class Calculator(object):
	    def add(self, x, y):
	        return x + y
	    def raiseError(self):
	        raise Exception("An Error Happened!")

Then you need to create a queue consumer on your worker side:

	import redique
	calculator = Calculator()
	queue = redique.RediQue("calculator")
	queue.consume_loop(calculator)

The last statement will block forever consuming tasks as they arrive.

On the publisher machine you need to execute tasks remotely

	import redique
	queue = redique.RediQue("calculator")
	task_id = queue.push_task("add", 1, 2)
	print queue.get_task_state(task_id)
	print queue.wait_task_result(task_id)

Another way to do that is to call execute_task that blocks till the result is returned

	import redique
	queue = redique.RediQue("calculator")
	print queue.execute_task("add", 1, 2)
 
[Ahmed Soliman][]
