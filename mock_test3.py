Que 1.Implement a stack using a list in Python. 
Include the necessary methods such as push, pop, and isEmpty.

Solution:-

class Stack:
    def __init__(self):
        self.stack = []

    def push(self,item):
        self.stack.append(item)

    def pop(self):
        if not self.is_empty():
            return self.stack.pop()
        else:
            raise IndexError("Stack is empty")
        
    def is_empty(self):
        return len(self.stack)=0


Que 2. Implement a queue using a list in Python. 
Include the necessary methods such as enqueue, dequeue, and isEmpty.

Solution:

class Queue:
    def __init__(self):
        self.queue = []

    def enqueue(self,item):
        self.queue.append(item)

    def dequeue(self):
        if not self.is_empty():
            return self.queue.pop(0)
        else:
            raise IndexError("Queue is empty")
        
    def is_empty(self):
        return len(self.queue)==0



