import unittest
import logging
import redique, time
import threading
logging.basicConfig(level=logging.DEBUG)

class SampleBackend(object):
    def add(self, x, y):
        return x+y
    
    def concat(self, str1, str2):
        return "%s%s" % (str1, str2)
    
    def getval(self):
        return 66
    
    def fail(self):
        raise Exception("Beep, I just failed!")
    
class TestBasics(unittest.TestCase):
    def setUp(self):
        self.client = redique.RediQue("redique_test", timeout=2)
        self.client.flush()
        
    def tearDown(self):
        self.client.flush()
    
    def testBasicScenario(self):
        backend = SampleBackend()
        threading.Thread(target=self.client.consume_one, args=[backend]).start()
        self.assertEquals(0, len(self.client))
        result = self.client.execute_task(backend.add, 1, 2)
        self.assertEquals(3, result)

        threading.Thread(target=self.client.consume_one, args=[backend]).start()
        result = self.client.execute_task(backend.add, 5, 20)
        self.assertEquals(25, result)
        self.assertEquals(0, len(self.client))
        
        threading.Thread(target=self.client.consume_one, args=[backend]).start()
        result = self.client.execute_task(backend.concat, "Ahmed", "Soliman")
        self.assertEquals("AhmedSoliman", result)
        self.assertEquals(0, len(self.client))        

        threading.Thread(target=self.client.consume_one, args=[backend]).start()
        result = self.client.execute_task("concat", "Hazem", "Imam")
        self.assertEquals("HazemImam", result)
        self.assertEquals(0, len(self.client))        

        threading.Thread(target=self.client.consume_one, args=[backend]).start()
        result = self.client.execute_task("getval")
        self.assertEquals(66, result)
        self.assertEquals(0, len(self.client))       
        
    def testAsynOperations(self):
        backend = SampleBackend()
        self.assertEquals(0, len(self.client))
        id1 = self.client.push_task(backend.add, 1, 2)
        id2 = self.client.push_task(backend.add, 2, 4)
        id3 = self.client.push_task(backend.add, 5, 2)
        self.assertEquals(redique.TASK_NEW, self.client.get_task_state(id3)['state'])
        id3_ts = self.client.get_task_state(id3)['timestamp']
        self.assertEquals(redique.TASK_NEW, self.client.get_task_state(id2)['state'])
        self.assertEquals(redique.TASK_NEW, self.client.get_task_state(id1)['state'])
        self.client.consume_one(backend)
        self.assertEquals(redique.TASK_DONE, self.client.get_task_state(id1)['state'])
        self.assertEqual(3, self.client.read_task_result(id1))
        self.assertEquals(None, self.client.get_task_state(id1))

        self.client.consume_one(backend)
        self.assertEquals(redique.TASK_DONE, self.client.get_task_state(id2)['state'])
        self.assertEqual(6, self.client.read_task_result(id2))
        self.assertEquals(None, self.client.get_task_state(id2))
        time.sleep(0.1)
        self.client.consume_one(backend)
        self.assertEquals(redique.TASK_DONE, self.client.get_task_state(id3)['state'])
        self.assertTrue(self.client.get_task_state(id3)['timestamp'] > id3_ts)
        self.assertEqual(7, self.client.read_task_result(id3))
        self.assertEquals(None, self.client.get_task_state(id3))
        self.assertEquals(0, len(self.client))
    
    def testFailure(self):
        backend = SampleBackend()
        self.assertEquals(0, len(self.client))
        threading.Thread(target=self.client.consume_one, args=[backend]).start()
        try:
            _ = self.client.execute_task(backend.fail)
        except Exception, e:
            self.assertEquals("Beep, I just failed!", str(e))
        else:
            self.fail("Exception was not thrown")
    
    def testFlush(self):
        backend = SampleBackend()
        self.assertEquals(0, len(self.client))
        self.client.push_task(backend.add, 1, 2)
        self.client.push_task(backend.add, 2, 4)
        self.client.push_task(backend.add, 5, 2)
        self.assertEquals(3, len(self.client))
        self.assertEquals(4, len(self.client._redis.keys('redique:redique_test*')))
        self.client.flush()
        self.assertEquals(0, len(self.client._redis.keys('redique:redique_test*')))